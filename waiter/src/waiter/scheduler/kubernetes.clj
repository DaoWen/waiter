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
            [waiter.mesos.utils :as mesos-utils] ;; XXX - rename/refactor the mesos.utils namespace
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (org.joda.time.format DateTimeFormat)))

(defmacro k8s-log [& args]
  `(log/log "Kubernetes" :debug nil (print-str ~@args)))

(def k8s-api-auth-str (atom nil))

(def k8s-timestamp-format
  "Kubernetes reports dates in ISO8061 format, sans the milliseconds component."
  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(defn- timestamp-str->datetime [k8s-timestamp-str]
  (du/str-to-date k8s-timestamp-str k8s-timestamp-format))

(defn- use-short-service-hash? [k8s-name-max-length]
  ;; This is fairly arbitrary, but if we have at least 48 characters for the app name,
  ;; then we can fit the full 32 character service-id hash, plus a hyphen as a separator,
  ;; and still have 25 characters left for some prefix of the app name.
  ;; If we have fewer than 48 characters, then we'll probably want to shorten the hash.
  (< k8s-name-max-length 48))

(def pod-unique-suffix-length
  "Kuberentes Pods have a unique 5-character alphanumeric suffix preceded by a hyphen."
  5)

(defn- service-id->k8s-name [service-id {:keys [name-max-length] :as scheduler}]
  (let [[_ app-prefix x y z] (re-find #"([^-]+)-(\w{8})(\w+)(\w{8})$" service-id)
        k8s-name-max-length (- name-max-length pod-unique-suffix-length 1)
        suffix (if (use-short-service-hash? k8s-name-max-length)
                 (str \- x z)
                 (str \- x y z))
        prefix-max-length (- k8s-name-max-length (count suffix))
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
    (scheduler/make-Service
      {:k8s-name (get-in replicaset [:metadata :name])
       :id (get-in replicaset [:metadata :annotations :waiter/service-id])
       :instances started
       :namespace (get-in replicaset [:metadata :namespace])
       :task-count requested
       :task-stats {:healthy available
                    :running ready
                    :staged (- requested ready)
                    :unhealthy unavailable}})))

(defn- pod->instance-id
  ([pod] (pod->instance-id pod (get-in pod [:status :containerStatuses 0 :restartCount])))
  ([pod restart-count]
   (let [pod-name (get-in pod [:metadata :name])
         instance-suffix (subs pod-name (- (count pod-name) pod-unique-suffix-length))
         service-id (get-in pod [:metadata :annotations :waiter/service-id])]
     (str service-id \. instance-suffix \- restart-count))))

(defn- killed-by-k8s? [pod-terminated-info]
  ;; TODO - Look at events for messages about liveness probe failures:
  ;; /api/v1/namespaces/<ns>/events?fieldSelector=involvedObject.namespace=<ns>,involvedObject.name=<instance-id>,reason=Unhealthy
  ;; (-> event :message (string/starts-with? "Liveness probe failed:")) #{:never-passed-health-checks}
  ;; For now, we just assume any SIGKILL (137) with the default "Error" reason was a livenessProbe kill.
  (and (= 137 (:exitCode pod-terminated-info))
       (= "Error" (:reason pod-terminated-info))))

(defn- track-failed-instances [{:keys [service-id] :as live-instance} pod {:keys [service-id->failed-instances-transient-store]}]
  (when-let [newest-failure (get-in pod [:status :containerStatuses 0 :lastState :terminated])]
    (let [failure-flags (if (= "OOMKilled" (:reason newest-failure)) #{:memory-limit-exceeded} #{})
          newest-failure-start-time (-> newest-failure :startedAt timestamp-str->datetime)
          restart-count (get-in pod [:status :containerStatuses 0 :restartCount])
          newest-failure-id (pod->instance-id pod (dec restart-count))
          failures (-> service-id->failed-instances-transient-store deref (get service-id))]
      (when-not (contains? failures newest-failure-id)
        (let [newest-failure-instance (merge live-instance
                                             ; to match the behavior of the marathon scheduler,
                                             ; don't include the exit code in failed instances that were killed by k8s
                                             (when-not (killed-by-k8s? newest-failure)
                                               {:exit-code (:exitCode newest-failure)})
                                             {:flags failure-flags
                                              :healthy? false
                                              :id newest-failure-id
                                              :started-at newest-failure-start-time})]
          (swap! service-id->failed-instances-transient-store
                 update-in [service-id] assoc newest-failure-id newest-failure-instance))))))

(defn- pod->ServiceInstance
  [pod]
  ;; FIXME - catch exceptions and return nil
  (let [port0 (get-in pod [:spec :containers 0 :ports 0 :containerPort])]
    (scheduler/make-ServiceInstance
      {:k8s-name (get-in pod [:metadata :labels :app])
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
  [client url & {:keys [body content-type request-method] :as options}]
  (k8s-log "Making request to K8s API server:" url request-method body)
  (ss/try+
    (let [auth-str @k8s-api-auth-str
          result (pc/mapply mesos-utils/http-request client url
                            :accept "application/json"
                            (cond-> options
                              auth-str (assoc-in [:headers "Authorization"] auth-str)
                              (and (not content-type ) body) (assoc :content-type "application/json")))]
      (k8s-log "Response from K8s API server:" (as-json result))
      result)
    (catch [:status 400] _
      (log/error "malformed request: " url options))
    (catch [:client client] response
      (log/error "Request to K8s API server failed: " url options body response)
      (ss/throw+ response))))

(defn- service-description->namespace
  [service-description]
  (get service-description "run-as-user"))

(defn- get-services
  [{:keys [api-server-url http-client namespaces-fn] :as scheduler}]
  (let [query-url-prefix (str api-server-url "/apis/extensions/v1beta1")
        query-url-suffix "/replicasets?labelSelector=managed-by=waiter"
        query-urls (if namespaces-fn
                     (for [n (namespaces-fn)]
                       (str query-url-prefix "/namespaces/" n query-url-suffix))
                     [(str query-url-prefix query-url-suffix)])]
    (vec
      (for [query-url query-urls
            item (->> query-url (api-request http-client) :items)]
        (replicaset->Service item)))))


(defn- get-replicaset-pods
  [api-server-url http-client {:keys [k8s-name namespace] :as service}]
  (->> (str api-server-url
            "/api/v1/namespaces/"
            namespace
            "/pods?labelSelector=app="
            k8s-name)
       (api-request http-client)
       :items))

(defn- get-service-instances
  [service {:keys [api-server-url http-client] :as scheduler}]
  (vec (for [pod (get-replicaset-pods api-server-url http-client service)
             :let [service-instance (pod->ServiceInstance pod)]
             :when (:host service-instance)]
         (doto service-instance
           (track-failed-instances pod scheduler)))))

(defn- instances-breakdown
  [service {:keys [service-id->failed-instances-transient-store] :as scheduler}]
  {:active-instances (get-service-instances service scheduler)
   :failed-instances (vals (get @service-id->failed-instances-transient-store (:id service) []))})

(defn- patch-object-json
  [k8s-object-uri http-client ops]
  (api-request http-client k8s-object-uri
               :body (as-json ops)
               :content-type "application/json-patch+json"
               :request-method :patch))

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
  (let [request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (:namespace service)
                         "/replicasets/"
                         (:k8s-name service))
        instances (:task-count service)]
    (patch-object-replicas http-client request-url instances instances')))

(defn- scale-service-by
  [api-server-url http-client service instances-delta]
  (let [request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (:namespace service)
                         "/replicasets/"
                         (:k8s-name service))
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
  [service-id service-description scheduler service-id->password-fn]
  (let [spec-file-path "./specs/k8s-default-pod.edn" ; TODO - allow user to provide this via the config file
        {:strs [backend-proto cmd cpus grace-period-secs health-check-interval-secs
                health-check-max-consecutive-failures mem min-instances ports run-as-user]} service-description
        home-path (str "/home/" run-as-user)
        common-env (scheduler/environment service-id service-description
                                          service-id->password-fn home-path)
        port0 8080 ;; TODO - get this port number from scheduler settings
        template-env (into [;; We set these two "MESOS_*" variables to improve interoperability
                            {:name "MESOS_DIRECTORY", :value home-path}
                            {:name "MESOS_SANDBOX", :value home-path}]
                           (concat
                             (for [[k v] common-env]
                               {:name k, :value v})
                             (for [i (range ports)]
                               {:name (str "PORT" i), :value (str (+ port0 i))})))
        params {:k8s-name (service-id->k8s-name service-id scheduler)
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
        edn-opts {:readers {'waiter/param params
                            'waiter/param-str (comp str params)
                            'waiter/port #(+ port0 %)
                            'waiter.fn/into (fn [[xs ys]] (into xs ys))
                            'waiter.fn/lower-case string/upper-case
                            'waiter.fn/str #(apply str %)
                            'waiter.fn/upper-case string/upper-case}}]
    (->> spec-file-path slurp (edn/read-string edn-opts))))

(defn- create-service
  [service-id descriptor {:keys [api-server-url http-client] :as scheduler} service-id->password-fn]
  (let [{:strs [run-as-user] :as service-description} (:service-description descriptor)
        spec-json (service-spec service-id service-description scheduler service-id->password-fn)
        request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (service-description->namespace service-description)
                         "/replicasets")
        response-json (api-request http-client request-url
                                   :body (as-json spec-json)
                                   :request-method :post)]
    (replicaset->Service response-json)))

(defn- delete-service
  [api-server-url http-client service]
  (when-not service
    (ss/throw+ {:status 404, :message "Service not found"}))
  (let [replicaset-url (str api-server-url
                            "/apis/extensions/v1beta1/namespaces/"
                            (:namespace service)
                            "/replicasets/"
                            (:k8s-name service))]
    ; FIXME - catch and handle exceptions
    (patch-object-json replicaset-url http-client
                       [{:op :replace, :path "/metadata/annotations/waiter~1app-status", :value "killed"}
                        {:op :replace, :path "/spec/replicas", :value 0}])
    (doseq [pod (get-replicaset-pods api-server-url http-client service)
            :let [pod-url (->> pod :metadata :selfLink (str api-server-url))]]
      (api-request http-client pod-url :request-method :delete))
    (api-request http-client replicaset-url :request-method :delete)
    {:message (str "K8s deleted ReplicaSet " (:k8s-name service))
     :result :deleted}))

(defn- service-id->service
  [{:keys [api-server-url http-client service-id->service-description-fn] :as scheduler} service-id]
  (let [service-ns (-> service-id service-id->service-description-fn service-description->namespace)
        replicasets (->> (str api-server-url
                              "/apis/extensions/v1beta1/namespaces/"
                              service-ns
                              "/replicasets?labelSelector=managed-by=waiter,app="
                              (service-id->k8s-name service-id scheduler))
                         (api-request http-client)
                         :items)]
    (when (seq replicasets)
      (-> replicasets first replicaset->Service))))

; The KubernetesScheduler
(defrecord KubernetesScheduler [api-server-url http-client
                                name-max-length
                                service-id->failed-instances-transient-store
                                service-id->service-description-fn]
  scheduler/ServiceScheduler

  (get-apps->instances [this]
    (->> this
         get-services
         (pc/map-from-keys #(instances-breakdown % this))))

  (get-apps [this]
    (get-services this))

  (get-instances [this service-id]
    (let [service (service-id->service this service-id)]
      (instances-breakdown service this)))

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
    (let [service-id (:service-id descriptor)]
      (when-not (scheduler/app-exists? this service-id)
        (ss/try+
          (create-service service-id descriptor this service-id->password-fn)
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
    {:failed-instances (vals (get @service-id->failed-instances-transient-store service-id))
     :killed-instances (scheduler/service-id->killed-instances service-id)})

  (state [_]
    {:service-id->failed-instances @service-id->failed-instances-transient-store}))

(defn- start-auth-renewer
  [{:keys [refresh-delay-minutes refresh-fn]}]
  (let [refresh (-> refresh-fn utils/resolve-symbol deref)
        auth-update-fn (fn auth-update []
                         (if-let [auth-str' (refresh)]
                           (reset! k8s-api-auth-str auth-str')))]
    (auth-update-fn)
    (when [refresh-delay-minutes]
      (assert (utils/pos-int? refresh-delay-minutes))
      (-> refresh-delay-minutes
          t/minutes
          (du/start-timer-task auth-update-fn)))))

(defn kubernetes-scheduler
  "Returns a new KubernetesScheduler with the provided configuration. Validates the
  configuration against kubernetes-scheduler-schema and throws if it's not valid."
  [{:keys [authentication http-options force-kill-after-ms framework-id-ttl
           service-id->service-description-fn url]}]
  {:pre [(utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))]}
  (let [http-client (mesos-utils/http-client-factory http-options)
        name-max-length 63
        service-id->failed-instances-transient-store (atom {})]
    (when authentication
      (start-auth-renewer authentication))
    (->KubernetesScheduler url http-client
                           name-max-length
                           service-id->failed-instances-transient-store
                           service-id->service-description-fn)))
