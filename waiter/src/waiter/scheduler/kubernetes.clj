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
            [waiter.util.http-utils :as http-utils] ;; XXX - rename/refactor the mesos.utils namespace
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (org.joda.time.format DateTimeFormat)))

(defmacro k8s-log
  "Log Kuberenets-specific messages."
  [& args]
  `(log/log "Kubernetes" :debug nil (print-str ~@args)))

(def k8s-api-auth-str
  "Atom containing authentication string for the Kubernetes API server.
   This value may be periodically refereshed asynchronously."
  (atom nil))

(def k8s-timestamp-format
  "Kubernetes reports dates in ISO8061 format, sans the milliseconds component."
  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(defn- timestamp-str->datetime
  "Parse a Kuberentes API timestamp string."
  [k8s-timestamp-str]
  (du/str-to-date k8s-timestamp-str k8s-timestamp-format))

(defn- use-short-service-hash? [k8s-max-name-length]
  ;; This is fairly arbitrary, but if we have at least 48 characters for the app name,
  ;; then we can fit the full 32 character service-id hash, plus a hyphen as a separator,
  ;; and still have 25 characters left for some prefix of the app name.
  ;; If we have fewer than 48 characters, then we'll probably want to shorten the hash.
  (< k8s-max-name-length 48))

;; Kuberentes Pods have a unique 5-character alphanumeric suffix preceded by a hyphen.
(def ^:const pod-unique-suffix-length 5)

(defn- service-id->k8s-name [{:keys [max-name-length] :as scheduler} service-id]
  "Shorten a full Waiter service-id to a Kubernetes-compatible application name.
   May return the service-id unmodified if it doesn't violate the
   configured name-length restrictions for this Kubernetes cluster."
  (let [[_ app-prefix x y z] (re-find #"([^-]+)-(\w{8})(\w+)(\w{8})$" service-id)
        k8s-max-name-length (- max-name-length pod-unique-suffix-length 1)
        suffix (if (use-short-service-hash? k8s-max-name-length)
                 (str \- x z)
                 (str \- x y z))
        prefix-max-length (- k8s-max-name-length (count suffix))
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

(pc/defnk replicaset->Service
  "Convert a Kuberentes ReplicaSet JSON response into a Waiter Service record."
  [spec
   [:metadata name namespace [:annotations waiter/service-id]]
   [:status {replicas 0} {availableReplicas 0} {readyReplicas 0} {unavailableReplicas 0}]]
  (let [requested (get spec :replicas 0)
        staged (- (+ availableReplicas unavailableReplicas) replicas)]
    (scheduler/make-Service
      {:k8s-name name
       :id service-id
       :instances requested
       :namespace namespace
       :task-count replicas
       :task-stats {:healthy readyReplicas
                    :running replicas
                    :staged staged
                    :unhealthy (- replicas readyReplicas staged)}})))

(defn- pod->instance-id
  "Construct the Waiter instance-id for the given Kubernetes pod incarnation.
   Note that a new Waiter Service Instance is created each time a pod restarts."
  ([pod] (pod->instance-id pod (get-in pod [:status :containerStatuses 0 :restartCount])))
  ([pod restart-count]
   (let [pod-name (get-in pod [:metadata :name])
         instance-suffix (subs pod-name (- (count pod-name) pod-unique-suffix-length))
         service-id (get-in pod [:metadata :annotations :waiter/service-id])]
     (str service-id \. instance-suffix \- restart-count))))

(defn- killed-by-k8s?
  "Determine whether a pod was killed (restarted) by its corresponding Kubernetes liveness checks."
  [pod-terminated-info]
  ;; TODO (#351) - Look at events for messages about liveness probe failures.
  ;; Currently, we assume any SIGKILL (137) with the default "Error" reason was a livenessProbe kill.
  (and (= 137 (:exitCode pod-terminated-info))
       (= "Error" (:reason pod-terminated-info))))

(defn- track-failed-instances!
  "Update this KuberentesScheduler's service-id->failed-instances-transient-store
   when a new pod failure is listed in the given pod's lastState container status."
  [{:keys [service-id] :as live-instance} {:keys [service-id->failed-instances-transient-store]} pod]
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
  "Convert a Kuberentes Pod JSON response into a Waiter Service Instance record."
  [pod]
  (try
    (let [port0 (get-in pod [:spec :containers 0 :ports 0 :containerPort])]
      (scheduler/make-ServiceInstance
        {:k8s-name (get-in pod [:metadata :labels :app])
         :extra-ports (->> (get-in pod [:metadata :annotations :waiter/port-count])
                           Integer/parseInt range next (mapv #(+ port0 %)))
         :healthy? (get-in pod [:status :containerStatuses 0 :ready] false)
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
                         (get-in [:status :startTime])
                         (timestamp-str->datetime))}))
    (catch Throwable e
      (log/error e "Error converting pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn api-request
  "Make an HTTP request to the Kuberenets API server using the configured authentication.
   If data is provided via :body, the application/json content type is added automatically.
   The response payload (if any) is automatically parsed to JSON."
  [client url & {:keys [body content-type request-method] :as options}]
  (k8s-log "Making request to K8s API server:" url request-method body)
  (ss/try+
    (let [auth-str @k8s-api-auth-str
          result (pc/mapply http-utils/http-request client url
                            :accept "application/json"
                            (cond-> options
                              auth-str (assoc-in [:headers "Authorization"] auth-str)
                              (and (not content-type ) body) (assoc :content-type "application/json")))]
      (k8s-log "Response from K8s API server:" (as-json result))
      result)
    (catch [:status 400] _
      (log/error "Malformed K8s API request: " url options))
    (catch [:client client] response
      (log/error "Request to K8s API server failed: " url options body response)
      (ss/throw+ response))))

(defn- service-description->namespace
  [service-description]
  (get service-description "run-as-user"))

(defn- get-services
  "Get all Waiter Services (reified as ReplicaSets) running in this Kuberentes cluster."
  [{:keys [api-server-url http-client] :as scheduler}]
  (->>
    "/apis/extensions/v1beta1/replicasets?labelSelector=managed-by=waiter"
    (str api-server-url)
    (api-request http-client)
    :items
    (mapv replicaset->Service)))

(defn- get-replicaset-pods
  "Get all Kuberentes pods associated with the given Waiter Service's corresponding ReplicaSet."
  [{:keys [api-server-url http-client service-id->service-description-fn] :as scheduler} {:keys [k8s-name namespace]}]
  (->> (str api-server-url
            "/api/v1/namespaces/"
            namespace
            "/pods?labelSelector=app="
            k8s-name)
       (api-request http-client)
       :items))

(defn- get-service-instances
  "Get all Waiter Service Instances associated with the given Waiter Service."
  [{:keys [api-server-url http-client] :as scheduler} basic-service-info]
  (vec (for [pod (get-replicaset-pods scheduler basic-service-info)
             :let [service-instance (pod->ServiceInstance pod)]
             :when (:host service-instance)]
         (doto service-instance
           (track-failed-instances! scheduler pod)))))

(defn instances-breakdown
  "Get all Waiter Service Instances associated with the given Waiter Service.
   Grouped by liveness status, i.e.: {:active-instances [...] :failed-instances [...]}."
  [{:keys [service-id->failed-instances-transient-store] :as scheduler} {service-id :id :as basic-service-info}]
  {:active-instances (get-service-instances scheduler basic-service-info)
   :failed-instances (-> @service-id->failed-instances-transient-store (get service-id []) vals vec)
   :killed-instances (-> service-id scheduler/service-id->killed-instances vec)})

(defn- patch-object-json
  "Make a JSON-patch request on a given Kubernetes object."
  [k8s-object-uri http-client ops]
  (api-request http-client k8s-object-uri
               :body (as-json ops)
               :content-type "application/json-patch+json"
               :request-method :patch))

(defn- patch-object-replicas
  "Update the replica count in the given Kubernetes object's spec."
  [k8s-object-uri http-client replicas replicas']
  (patch-object-json http-client k8s-object-uri
                     ;; NOTE: ~1 is JSON-patch escape syntax for a "/" in a key name.
                     ;; See http://jsonpatch.com/#json-pointer
                     [{:op :test :path "/metadata/annotations/waiter~1app-status" :value "live"}
                      {:op :test :path "/spec/replicas" :value replicas}
                      {:op :replace :path "/spec/replicas" :value replicas'}]))

(defn- get-replica-count
  "Query the current replica count for the given Kubernetes object."
  [{:keys [http-client] :as scheduler} replicaset-url]
  (-> (api-request http-client replicaset-url)
      (get-in [:spec :replicas])))

(defmacro k8s-patch-with-retries
  "Query the current replica count for the given Kubernetes object,
   retrying a limited number of times in the event of an HTTP 409 conflict error."
  [patch-cmd retry-condition retry-cmd]
  `(let [patch-result# (ss/try+
                         ~patch-cmd
                         (catch [:status 409] _#
                           ::conflict))]
     (if (not= ::conflict patch-result#)
       patch-result#
       (if ~retry-condition
         ~retry-cmd
         (ss/throw+ {:status 409})))))

(defn- scale-service-up-to
  "Scale the number of instances for a given service to a specific number.
   Only used for upward scaling. No-op if it would result in downward scaling."
  [{:keys [api-server-url http-client max-conflict-retries] :as scheduler} service instances']
  (let [replicaset-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (:namespace service)
                         "/replicasets/"
                         (:k8s-name service))]
    (loop [attempt 1
           instances (:instances service)]
      (if (<= instances' instances)
        (log/info "Skipping non-upward scale-up request on" (:id service)
                  "from" instances "to" instances')
        (k8s-patch-with-retries
          (patch-object-replicas http-client replicaset-url instances instances')
          (< attempt max-conflict-retries)
          (recur (inc attempt) (get-replica-count scheduler replicaset-url)))))))

(defn- scale-service-by-delta
  "Scale the number of instances for a given service by a given delta.
   Can scale either upward (positive delta) or downward (negative delta)."
  [{:keys [api-server-url http-client max-conflict-retries] :as scheduler} service instances-delta]
  (let [replicaset-url (str api-server-url
                            "/apis/extensions/v1beta1/namespaces/"
                            (:namespace service)
                            "/replicasets/"
                            (:k8s-name service))]
    (loop [attempt 1
           instances (:instances service)]
      (let [instances' (+ instances instances-delta)]
        (k8s-patch-with-retries
          (patch-object-replicas http-client replicaset-url instances instances')
          (< attempt max-conflict-retries)
          (recur (inc attempt) (get-replica-count scheduler replicaset-url)))))))

(defn- kill-service-instance
  "Safely kill the Kubernetes pod corresponding to the given Waiter Service Instance.
   Returns 200 (OK) on success, but throws on failure."
  [{:keys [api-server-url http-client] :as scheduler} {:keys [id namespace pod-name service-id] :as instance} service]
  (let [pod-url (str api-server-url
                     "/api/v1/namespaces/"
                     namespace
                     "/pods/"
                     pod-name)
        base-body {:kind "DeleteOptions" :apiVersion "v1"}
        term-json (-> base-body (assoc :gracePeriodSeconds 300) (as-json))
        kill-json (-> base-body (assoc :gracePeriodSeconds 0) (as-json))
        make-kill-response (fn [killed? message status]
                             {:instance-id id :killed? killed?
                              :message message :service-id service-id :status status})]
    ; request termination of the instance
    (api-request http-client pod-url :request-method :delete :body term-json)
    ; scale down the replicaset to reflect removal of this instance
    (scale-service-by-delta scheduler service -1)
    ; force-kill the instance (should still be terminating)
    (api-request http-client pod-url :request-method :delete :body kill-json)
    ; report back that the instance was killed successfully
    (scheduler/process-instance-killed! instance)
    200))

(defn- service-spec
  "Creates a Kubernetes ReplicaSet spec (with an embedded Pod spec) for the given Waiter Service."
  [{:keys [rs-spec-file-path pod-base-port] :as scheduler} service-id service-description service-id->password-fn]
  (let [{:strs [backend-proto cmd cpus grace-period-secs health-check-interval-secs
                health-check-max-consecutive-failures mem min-instances ports run-as-user]} service-description
        home-path (str "/home/" run-as-user)
        common-env (scheduler/environment service-id service-description
                                          service-id->password-fn home-path)
        port0 pod-base-port
        template-env (into [;; We set these two "MESOS_*" variables to improve interoperability
                            {:name "MESOS_DIRECTORY" :value home-path}
                            {:name "MESOS_SANDBOX" :value home-path}]
                           (concat
                             (for [[k v] common-env]
                               {:name k :value v})
                             (for [i (range ports)]
                               {:name (str "PORT" i) :value (str (+ port0 i))})))
        params {:k8s-name (service-id->k8s-name scheduler service-id)
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
    (try
      (->> rs-spec-file-path slurp (edn/read-string edn-opts))
      (catch Throwable e
        (log/error e "Error creating ReplicaSet specification for" service-id)
        (throw e)))))

(defn- create-service
  "Reify a Waiter Service as a Kuberentes ReplicaSet."
  [service-id descriptor {:keys [api-server-url http-client] :as scheduler} service-id->password-fn]
  (let [{:strs [run-as-user] :as service-description} (:service-description descriptor)
        spec-json (service-spec scheduler service-id service-description service-id->password-fn)
        request-url (str api-server-url
                         "/apis/extensions/v1beta1/namespaces/"
                         (service-description->namespace service-description)
                         "/replicasets")
        response-json (api-request http-client request-url
                                   :body (as-json spec-json)
                                   :request-method :post)]
    (when-not (= "ReplicaSet" (:kind response-json))
      (log/error "Invalid response from ReplicaSet create request"
                 (as-json response-json)))
    (replicaset->Service response-json)))

(defn- delete-service
  "Delete the Kubernetes ReplicaSet corresponding to a Waiter Service.
   Ensures that all controlled Pods are deleted before removing the ReplicaSet.
   This operation will leave Waiter in a sane state on a failure."
  [{:keys [api-server-url http-client] :as scheduler} service]
  (when-not service
    (log/error "Null service passed to kubernetes delete-service.")
    (ss/throw+ {:status 404 :message "Service not found"}))
  (let [replicaset-url (str api-server-url
                            "/apis/extensions/v1beta1/namespaces/"
                            (:namespace service)
                            "/replicasets/"
                            (:k8s-name service))]
    (patch-object-json replicaset-url http-client
                       [{:op :replace :path "/metadata/annotations/waiter~1app-status" :value "killed"}
                        {:op :replace :path "/spec/replicas" :value 0}])
    (doseq [pod (get-replicaset-pods scheduler service)
            :let [pod-url (->> pod :metadata :selfLink (str api-server-url))]]
      (api-request http-client pod-url :request-method :delete))
    (api-request http-client replicaset-url :request-method :delete)
    {:message (str "K8s deleted ReplicaSet " (:k8s-name service))
     :result :deleted}))

(defn service-id->service
  "Look up the Kubernetes ReplicaSet associated with a given Waiter service-id,
   and return a corresponding Waiter Service record."
  [{:keys [api-server-url http-client service-id->service-description-fn] :as scheduler} service-id]
  (try
    (when-let [service-ns (-> service-id service-id->service-description-fn service-description->namespace)]
      (let [replicasets (->> (str api-server-url
                                  "/apis/extensions/v1beta1/namespaces/"
                                  service-ns
                                  "/replicasets?labelSelector=managed-by=waiter,app="
                                  (service-id->k8s-name scheduler service-id))
                             (api-request http-client)
                             :items)]
        (when (seq replicasets)
          (when (second replicasets)
            (log/warn "Multiple matches found for Waiter Service"
                      service-id replicasets))
          (-> replicasets first replicaset->Service))))
    (catch Throwable e
      (log/error e "Error creating service record for service-id" service-id)
      (comment "Returning nil on failure."))))

; The Waiter Scheduler protocol implementation for Kubernetes
(defrecord KubernetesScheduler [api-server-url http-client
                                max-conflict-retries
                                max-name-length
                                pod-base-port
                                rs-spec-file-path
                                service-id->failed-instances-transient-store
                                service-id->service-description-fn]
  scheduler/ServiceScheduler

  (get-apps->instances [this]
    (->> this
         get-services
         (pc/map-from-keys (partial instances-breakdown this))))

  (get-apps [this]
    (get-services this))

  (get-instances [this service-id]
    (instances-breakdown this
                         {:id service-id
                          :k8s-name (service-id->k8s-name this service-id)
                          :namespace (-> service-id
                                         service-id->service-description-fn
                                         (get "run-as-user"))}))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (ss/try+
      (let [service (service-id->service this service-id)
            kill-result-status (kill-service-instance this instance service)]
        {:instance-id id
         :killed? true
         :message "Successfully killed instance"
         :service-id service-id
         :status  kill-result-status})
      (catch [:status 404] e
        {:instance-id id
         :killed? false
         :message "Instance not found"
         :service-id service-id
         :status 404})
      (catch [:status 409] e
        {:instance-id id
         :killed? false
         :message "Failed to update service specification due to repeated conflicts"
         :service-id service-id
         :status 409})
      (catch Throwable e
        {:instance-id id
         :killed? false
         :message (.getMessage e)
         :service-id service-id
         :status 500})))

  (app-exists? [this service-id]
    (some? (service-id->service this service-id)))

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
            delete-result (delete-service this service)]
        (swap! service-id->failed-instances-transient-store dissoc service-id)
        (scheduler/remove-killed-instances-for-service! service-id)
        {:result :deleted
         :message (str "Kubernetes deleted " service-id)})
      (catch [:status 404] _
        (log/warn "[delete-app] Service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Kubernetes reports service does not exist"})
      (catch [:status 409] e
        (log/warn "Kubernetes ReplicaSet conflict while deleting"
                  {:service-id service-id})
        {:result :conflict
         :message "Kubernetes ReplicaSet conflict while deleting"})))

  (scale-app [this service-id scale-to-instances]
    (ss/try+
      (scale-service-up-to
        this
        (service-id->service this service-id)
        scale-to-instances)
      {:success true
       :status 200
       :result :scaled
       :message (str "Scaled to " scale-to-instances)}
      (catch [:status 409] e
        {:success false
         :status 409
         :result :conflict
         :message "Scaling failed due to repeated patch conflicts"})
      (catch Throwable e
        (log/error e "Error while scaling waiter service" service-id)
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
  "Initialize the k8s-api-auth-str atom,
   and optionally start a chime to periodically referesh the value."
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
           max-conflict-retries max-name-length pod-base-port rs-spec-file-path
           service-id->service-description-fn url]}]
  {:pre [(utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))]}
  (let [http-client (http-utils/http-client-factory http-options)
        service-id->failed-instances-transient-store (atom {})]
    (when authentication
      (start-auth-renewer authentication))
    (->KubernetesScheduler url http-client
                           max-conflict-retries
                           max-name-length
                           pod-base-port
                           rs-spec-file-path
                           service-id->failed-instances-transient-store
                           service-id->service-description-fn)))
