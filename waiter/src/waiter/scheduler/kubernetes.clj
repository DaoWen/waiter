;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.scheduler.kubernetes
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.mesos.utils :as mesos-utils]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.util.utils :as utils])
  (:import (org.joda.time.format DateTimeFormat)))

(def service-id->failed-instances-transient-store (atom {}))
(def instance-x->failed-instances-transient-store (atom {}))
(def k8s-api-auth-str (atom nil))

;; XXX - this shouldn't be a global constant (it should be read from the config file)
(def k8s-name-length-limit 63)

(def k8s-timestamp-format (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(defn- timestamp-str->datetime [k8s-timestamp-str]
  (utils/str-to-date k8s-timestamp-str k8s-timestamp-format))

(defn- service-id->app-name [service-id]
  (let [[_ app-prefix x y z] (re-find #"([^-]+)-(\w{8})(\w+)(\w{8})$" service-id)
        app-name-max-length (- k8s-name-length-limit 6)
        suffix (if (< app-name-max-length 48)
                 (str \- x z)
                 (str \- x y z))
        prefix-max-length (- app-name-max-length (count suffix))
        app-prefix' (cond-> app-prefix
                      (< prefix-max-length (count app-prefix))
                      (subs 0 prefix-max-length))]
      (str app-prefix' suffix)))

(defn- kw->str
  "Convert a keyword to a string, preserving the keyword's namespace (if any)."
  [^clojure.lang.Keyword kw]
  (str (.-sym kw)))

(defn- as-json
  "Convert Clojure data structures to JSON, preserving namespaces on keys."
  [data]
  (json/write-str data :key-fn #(if (keyword? %) (kw->str %) %)))

(defn- replicaset->Service
  ;; FIXME - catch exceptions and return nil
  [replicaset]
  (let [replicaset-status (:status replicaset)
        ready (:readyReplicas replicaset-status 0)
        requested (get-in replicaset [:spec :replicas])
        started (:replicas replicaset-status 0)
        available (:availableReplicas replicaset-status 0)
        unavailable (:unavailableReplicas replicaset-status 0)]
    (log/info "ReplicaSet info" replicaset)
    (scheduler/make-Service
      {:app-name (get-in replicaset [:metadata :name])
       :id (get-in replicaset [:metadata :annotations :waiter/service-id])
       :instances ready
       :namespace (get-in replicaset [:metadata :namespace])
       :task-count requested
       :task-stats {:running ready
                    :staged (- requested ready)
                    :healthy available
                    :unhealthy unavailable}})))

(defn- pod->instance-id
  ([pod] (pod->instance-id pod (get-in pod [:status :containerStatuses 0 :restartCount])))
  ([pod restart-count]
   (let [pod-name (get-in pod [:metadata :name])
         instance-suffix (subs pod-name (- (count pod-name) 5))
         service-id (get-in pod [:metadata :annotations :waiter/service-id])]
     (str service-id \. instance-suffix \- restart-count))))

(defn- track-failed-instances [live-instance pod]
  (when-let [newest-failure (get-in pod [:status :containerStatuses 0 :lastState :terminated])]
    (let [service-id (:service-id live-instance)
          instance-id (:id live-instance)
          instance-x (pod->instance-id pod \X)
          newest-failure-start-time (-> newest-failure :startedAt timestamp-str->datetime)
          failures (-> instance-x->failed-instances-transient-store
                       deref (get instance-x))
          prev-failure-start-time (some-> failures last :started-at)]
      ;; FIXME - this is racey! we can end up reporting the same failed instances twice.
      (when (or (nil? prev-failure-start-time)
                (neg? (compare prev-failure-start-time newest-failure-start-time)))
        (let [failure-flags (cond
                              (= "OOMKilled" (:reason newest-failure)) #{:memory-limit-exceeded}
                              :else #{})
              ;; TODO - Look at events for messages about liveness probe failures:
              ;; /api/v1/namespaces/<ns>/events?fieldSelector=involvedObject.namespace=<ns>,involvedObject.name=<instance-id>,reason=Unhealthy
              ;; (-> event :message (string/starts-with? "Liveness probe failed:")) #{:never-passed-health-checks}
              ;; For now, we just assume any SIGKILL (137) with the default "Error" reason was a livenessProbe kill.
              killed-by-k8s? (and (= 137 (:exitCode newest-failure))
                                  (= "Error" (:reason newest-failure)))
              newest-failure-instance (merge live-instance
                                             ; to match the behavior of the marathon scheduler,
                                             ; don't include the exit code in failed instances that were killed by k8s
                                             (when-not killed-by-k8s?
                                               {:exit-code (:exitCode newest-failure)})
                                             {:flags failure-flags
                                              :healthy? false
                                              :id (pod->instance-id pod (count failures))
                                              :started-at newest-failure-start-time})]
          (doseq [[id store] [[service-id service-id->failed-instances-transient-store]
                              [instance-x instance-x->failed-instances-transient-store]]]
            (swap! store update-in [id] (comp vec conj) newest-failure-instance)))))))

(defn- pod->ServiceInstance
  [pod]
  ;; FIXME - catch exceptions and return nil
  (let [port0 (get-in pod [:spec :containers 0 :ports 0 :containerPort])]
  (scheduler/make-ServiceInstance
    {:app-name (get-in pod [:metadata :labels :app])
     :extra-ports (->> (get-in pod [:metadata :annotations :waiter/port-count])
                       Integer/parseInt range next (mapv #(+ port0 %)))
     :healthy? (get-in pod [:status :containerStatuses 0 :ready])
     :host (get-in pod [:status :podIP])
     :id (pod->instance-id pod)
     :log-directory (str "/home/" (get-in pod [:metadata :namespace]))
     :namespace (get-in pod [:metadata :namespace])
     :pod-name (get-in pod [:metadata :name])
     :port port0
     :protocol (get-in pod [:metadata :annotations :waiter/protocol])
     :restart-count (get-in pod [:status :containerStatuses 0 :restartCount])
     :service-id (get-in pod [:metadata :annotations :waiter/service-id])
     :started-at (-> pod
                     (get-in [:metadata :creationTimestamp])
                     (timestamp-str->datetime))})))

(defn- api-request
  [client url & {:keys [body content-type] :as options}]
  (ss/try+
    (log/info "K8s API request:" url options)
    (let [auth-str @k8s-api-auth-str
          result (pc/mapply mesos-utils/http-request client url
                            :accept "application/json"
                            (cond-> options
                              auth-str (assoc-in [:headers "Authorization"] auth-str)
                              (and (not content-type ) body) (assoc :content-type "application/json")))]
      (log/info "K8s API result:" result)
      result)
    (catch [:status 400] _
      (log/error "malformed request: " url options))))

(defn- service-description->namespace
  [service-description]
  (get service-description "run-as-user"))

(defn- get-services
  [api-server-url http-client]
  (->> (str api-server-url
            "/apis/extensions/v1beta1/replicasets?labelSelector=managed-by=waiter")
       (api-request http-client)
       :items
       (mapv replicaset->Service)))

(defn- get-replicaset-pods
  [api-server-url http-client {:keys [app-name namespace] :as service}]
  (->> (str api-server-url
            "/api/v1/namespaces/"
            namespace
            "/pods?labelSelector=app="
            app-name)
       (api-request http-client)
       :items))

(defn- get-service-instances
  [api-server-url http-client service]
  (vec (for [pod (get-replicaset-pods api-server-url http-client service)
             :let [service-instance (pod->ServiceInstance pod)]
             :when (:host service-instance)]
         (doto service-instance
           (track-failed-instances pod)))))

(defn- instances-breakdown
  [api-server-url http-client service]
  {:active-instances (get-service-instances api-server-url http-client service)
   :failed-instances (get @service-id->failed-instances-transient-store (:id service) [])})

(defn- service+instances
  [api-server-url http-client service]
  (let [instances (instances-breakdown api-server-url http-client service)]
    [service instances]))

(defn- patch-object-json
  [k8s-object-uri http-client ops]
  (api-request http-client k8s-object-uri
               :request-method :patch
               :content-type "application/json-patch+json"
               :body (as-json ops)))

(defn- patch-object-replicas
  [k8s-object-uri http-client replicas replicas']
  (patch-object-json http-client k8s-object-uri
                     ;; NOTE: ~1 is JSON-patch escape syntax for a "/" in a key name
                     ;; see http://jsonpatch.com/#json-pointer
                     [{:op :test, :path "/metadata/annotations/waiter~1app-status", :value "live"}
                      {:op :test, :path "/spec/replicas", :value replicas}
                      {:op :replace, :path "/spec/replicas", :value replicas'}]))

(defn- scale-service-to
  [api-server-url http-client service instances']
  (log/info "Scaling to " instances' service)
  (let [request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (:namespace service)
                         "/replicasets/"
                         (:app-name service))
        instances (:task-count service)]
    (patch-object-replicas http-client request-url instances instances')))

(defn- scale-service-by
  [api-server-url http-client service instances-delta]
  (log/info "Scaling incrementally by " instances-delta service)
  (let [request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (:namespace service)
                         "/replicasets/"
                         (:app-name service))
        instances (:task-count service)
        instances' (+ instances instances-delta)]
    (patch-object-replicas http-client request-url instances instances')))

(defn- kill-service-instance
  [api-server-url http-client {:keys [id namespace pod-name service-id] :as instance} service]
  (let [pod-url (str api-server-url
                     "/api/v1/namespaces/"
                     namespace
                     "/pods/"
                     pod-name)
        base-body {:kind "DeleteOptions", :apiVersion "v1"}
        term-json (-> base-body (assoc :gracePeriodSeconds 300) (as-json))
        kill-json (-> base-body (assoc :gracePeriodSeconds 0) (as-json))
        make-kill-response (fn [killed? message status]
                             {:instance-id id, :killed? killed?,
                              :message message, :service-id service-id, :status status})]
    ; request termination of the instance
    (api-request http-client pod-url :request-method :delete :body term-json)
    ; scale down the replicaset
    (scale-service-by api-server-url http-client service -1)
    ; force-kill the instance (should still be terminating)
    (api-request http-client pod-url :request-method :delete :body kill-json)
    ; report back that the instance was killed
    (scheduler/process-instance-killed! instance)
    (make-kill-response true "pod was killed" 200)))

(defn- service-spec
  [service-id service-description service-id->password-fn]
  (let [spec-file-path "./specs/k8s-default-pod.edn" ; TODO - allow user to provide this via the config file
        {:strs [backend-proto cmd cpus #_disk grace-period-secs health-check-interval-secs
                health-check-max-consecutive-failures mem ports min-instances
                #_restart-backoff-factor run-as-user]} service-description
        home-path (str "/home/" run-as-user)
        common-env (scheduler/environment service-id service-description
                                          service-id->password-fn home-path)
        port0 8080
        template-env (into [{:name "MESOS_DIR", :value home-path}
                            {:name "MESOS_SANDBOX", :value home-path}]
                           (concat
                             (for [[k v] common-env]
                               {:name k, :value v})
                             (for [i (range ports)]
                               {:name (str "PORT" i), :value (str (+ port0 i))})))
        params {:app-name (service-id->app-name service-id)
                :backend-protocol backend-proto
                :backend-protocol-caps (string/upper-case backend-proto)
                :cmd cmd
                :cpus cpus
                :env template-env
                :grace-period-secs grace-period-secs
                :health-check-interval-secs health-check-interval-secs
                :health-check-max-consecutive-failures health-check-max-consecutive-failures
                :health-check-url (sd/service-description->health-check-url service-description)
                :home-path home-path
                :memory  (str mem "Mi")
                :min-instances min-instances
                :port-count ports
                :run-as-user run-as-user
                :service-id service-id
                :ssl? (= "https" backend-proto)}
        delayed-values {:run-as-uid (->> run-as-user (shell/sh "id" "-u")
                                         :out string/trim Integer/parseInt delay)}
        edn-opts {:readers {'waiter/lookup (comp force delayed-values)
                            'waiter/param params
                            'waiter/param-str (comp str params)
                            'waiter/port #(+ port0 %)
                            'waiter.fn/into (fn [[xs ys]] (into xs ys))
                            'waiter.fn/lower-case string/upper-case
                            'waiter.fn/str #(apply str %)
                            'waiter.fn/upper-case string/upper-case}}]
    (->> spec-file-path slurp (edn/read-string edn-opts))))

(defn- create-service
  [api-server-url http-client service-id descriptor service-id->password-fn]
  (log/info "Creating service" service-id "with description" descriptor)
  (let [{:strs [run-as-user] :as service-description} (:service-description descriptor)
        spec-json (service-spec service-id service-description service-id->password-fn)
        request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (service-description->namespace service-description)
                         "/replicasets")
        response-json (api-request http-client request-url
                                     :request-method :post
                                     :body (as-json spec-json))]
    (replicaset->Service response-json)))

(defn- delete-service
  [api-server-url http-client service]
  (when-not service
    (ss/throw+ {:status 404, :message "Service not found"}))
  (let [replicaset-url (str api-server-url
                            "/apis/extensions/v1beta1/namespaces/"
                            (:namespace service)
                            "/replicasets/"
                            (:app-name service))]
    ; FIXME - catch and handle exceptions
    (patch-object-json replicaset-url http-client
                       [{:op :replace, :path "/metadata/annotations/waiter~1app-status", :value "killed"}
                        {:op :replace, :path "/spec/replicas", :value 0}])
    (doseq [pod (get-replicaset-pods api-server-url http-client service)
            :let [pod-url (->> pod :metadata :selfLink (str api-server-url))]]
      (api-request http-client pod-url :request-method :delete))
    (api-request http-client replicaset-url :request-method :delete)
    {:result :deleted
     :message (str "K8s deleted ReplicaSet " (:app-name service))}))

(defn- service-id->service
  [{:keys [api-server-url http-client service-id->service-description-fn]} service-id]
  (let [service-ns (-> service-id service-id->service-description-fn service-description->namespace)
        replicasets (->> (str api-server-url
                              "/apis/extensions/v1beta1/namespaces/"
                              service-ns
                              "/replicasets?labelSelector=managed-by=waiter,app="
                              (service-id->app-name service-id))
                         (api-request http-client)
                         :items)]
    (when (seq replicasets)
      (-> replicasets first replicaset->Service))))

; The KubernetesScheduler
(defrecord KubernetesScheduler [api-server-url http-client
                                service-id->service-description-fn]
  scheduler/ServiceScheduler

  (get-apps->instances [_]
    (let [apps (get-services api-server-url http-client)
          _ (log/info "apps =" apps)
          app->app+instances (partial service+instances api-server-url http-client)
          app->instances (into {} (mapv app->app+instances apps))
          _ (log/info "apps->instances =" app->instances)]
      app->instances))

  (get-apps [_]
    (get-services api-server-url http-client))

  (get-instances [this service-id]
    (let [service (service-id->service this service-id)]
      (instances-breakdown api-server-url http-client service)))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (ss/try+
      (let [service (service-id->service this service-id)
            kill-result (kill-service-instance api-server-url http-client instance service)]
        {:instance-id id
         :killed? true
         :message "Killed"
         :service-id service-id
         :status (:status kill-result)})
      (catch [:status 404] e
        {:instance-id id
         :killed? false
         :message (str id " not killed (instance not found)")
         :service-id service-id
         :status 404})))

  (app-exists? [this service-id]
    (ss/try+
      (some? (service-id->service this service-id))
      (catch [:status 404] _
        (log/warn "app-exists?: service" service-id "does not exist!"))))

  (create-app-if-new [this service-id->password-fn descriptor]
    (log/info "Try starting new app with descriptor" descriptor)
    (let [service-id (:service-id descriptor)]
      (when-not (scheduler/app-exists? this service-id)
        (ss/try+
          (log/info "Starting new app for" service-id "with descriptor" (dissoc descriptor :env))
          (create-service api-server-url http-client service-id descriptor service-id->password-fn)
          (catch [:status 409] e
            (log/warn (ex-info "Conflict status when trying to start app. Is app starting up?"
                               {:descriptor descriptor
                                :error e})
                      "Exception starting new app"))))))

  (delete-app [this service-id]
    (ss/try+
      (let [service (service-id->service this service-id)
            delete-result (delete-service api-server-url http-client service)]
        (comment ;; TODO - remove cached info about this service (once I start caching stuff)
                 (when delete-result
                   (remove-failed-instances-for-service! service-id->failed-instances-transient-store service-id)
                   (scheduler/remove-killed-instances-for-service! service-id)
                   (swap! service-id->kill-info-store dissoc service-id)))
        {:result :deleted
         :message (str "Kubernetes deleted " service-id)})
      (catch [:status 404] {}
        (log/warn "[delete-app] Service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Kubernetes reports service does not exist"})
      (catch [:status 409] e
        (log/warn "Kubernetes ReplicaSet conflict while deleting"
                  {:service-id service-id}))))

  (scale-app [this service-id scale-to-instances]
    (ss/try+
      (scale-service-to api-server-url http-client
                     (service-id->service this service-id)
                     scale-to-instances)
      {:success true
       :status 200
       :result :scaled
       :message (str "Scaled to " scale-to-instances)}
      (catch Throwable e
        {:success false
         :status 500
         :result :failed
         :message (str "Scaling failed: " (.getMessage e))})))

  (retrieve-directory-content [_ service-id instance-id _ relative-directory]
    ;; TODO - can I implement this?
    ;; I could just return the log for each pod:
    ;; /api/v1/namespaces/{ns}/pods/{instance-id}/log
    ;; I could also do this by starting a little python file server in the home directory.
    ;; I'd start it with a timeout of 5 minutes or something,
    ;; and if no requests come in for 5 minutes, then we kill it.
    [])

  (service-id->state [_ service-id]
    {:failed-instances (get @service-id->failed-instances-transient-store service-id [])
     :killed-instances (scheduler/service-id->killed-instances service-id)})

  (state [_]
    {:service-id->failed-instances @service-id->failed-instances-transient-store
     :instance-x->failed-instances @instance-x->failed-instances-transient-store}))

(defn kubernetes-scheduler
  "Returns a new KubernetesScheduler with the provided configuration. Validates the
  configuration against kubernetes-scheduler-schema and throws if it's not valid."
  [{:keys [authentication http-options force-kill-after-ms framework-id-ttl
           service-id->service-description-fn url]}]
  {:pre [(utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))]}
  (let [http-client (mesos-utils/http-client-factory http-options)]
    (when authentication
      (let [refresh-fn (-> authentication :refresh-fn utils/resolve-symbol deref)
            auth-update-fn (fn auth-update []
                             (if-let [auth-str' (refresh-fn)]
                               (reset! k8s-api-auth-str auth-str')))]
        (auth-update-fn)
        (when-let [refresh-delay-minutes (:refresh-delay-minutes authentication)]
          (assert (utils/pos-int? refresh-delay-minutes))
          (-> refresh-delay-minutes
              t/minutes
              (utils/start-timer-task auth-update-fn)))))
    (->KubernetesScheduler url http-client
                           service-id->service-description-fn)))
