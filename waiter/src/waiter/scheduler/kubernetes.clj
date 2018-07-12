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
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils])
  (:import (org.joda.time.format DateTimeFormat)))

(defn authorization-from-environment []
  "Sample implementation of the authentication string refresh function.
   Returns a string to be used as the value for the Authorization HTTP header,
   reading the string from the WAITER_K8S_AUTH_STRING environment variable."
  (log/info "Called waiter.scheduler.kubernetes/authorization-from-environment")
  (System/getenv "WAITER_K8S_AUTH_STRING"))

;; We use a 5-minute grace period on pods to enable manual victim selection on scale-down
(def ^:const delete-delay-secs 300)

(def k8s-api-auth-str
  "Atom containing authentication string for the Kubernetes API server.
   This value may be periodically refreshed asynchronously."
  (atom nil))

(def k8s-timestamp-format
  "Kubernetes reports dates in ISO8061 format, sans the milliseconds component."
  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(defn- timestamp-str->datetime
  "Parse a Kubernetes API timestamp string."
  [k8s-timestamp-str]
  (du/str-to-date k8s-timestamp-str k8s-timestamp-format))

(defn- use-short-service-hash? [k8s-max-name-length]
  ;; This is fairly arbitrary, but if we have at least 48 characters for the app name,
  ;; then we can fit the full 32 character service-id hash, plus a hyphen as a separator,
  ;; and still have 25 characters left for some prefix of the app name.
  ;; If we have fewer than 48 characters, then we'll probably want to shorten the hash.
  (< k8s-max-name-length 48))

;; Kubernetes Pods have a unique 5-character alphanumeric suffix preceded by a hyphen.
(def ^:const pod-unique-suffix-length 5)

(defn service-id->k8s-app-name [{:keys [max-name-length] :as scheduler} service-id]
  "Shorten a full Waiter service-id to a Kubernetes-compatible application name.
   May return the service-id unmodified if it doesn't violate the
   configured name-length restrictions for this Kubernetes cluster.

   Example:

   (service-id->k8s-app-name
     {:max-name-length 32}
     \"waiter-myapp-e8b625cc83c411e8974c38d5474b213d\")
   ==> \"myapp-e8b625cc474b213d\""
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

(pc/defnk replicaset->Service
  "Convert a Kubernetes ReplicaSet JSON response into a Waiter Service record."
  [spec
   [:metadata name namespace [:annotations waiter-service-id]]
   [:status {replicas 0} {availableReplicas 0} {readyReplicas 0} {unavailableReplicas 0}]]
  (let [requested (get spec :replicas 0)
        staged (- (+ availableReplicas unavailableReplicas) replicas)]
    (scheduler/make-Service
      {:id waiter-service-id
       :instances requested
       :k8s/app-name name
       :k8s/namespace namespace
       :task-count replicas
       :task-stats {:healthy readyReplicas
                    :running replicas
                    :staged staged
                    :unhealthy (- replicas readyReplicas staged)}})))

(defn- pod->instance-id
  "Construct the Waiter instance-id for the given Kubernetes pod incarnation.
   Note that a new Waiter Service Instance is created each time a pod restarts,
   and that we generate a unique instance-id by including the pod's restartCount value."
  ([pod] (pod->instance-id pod (get-in pod [:status :containerStatuses 0 :restartCount])))
  ([pod restart-count]
   (let [pod-name (get-in pod [:metadata :name])
         instance-suffix (subs pod-name (- (count pod-name) pod-unique-suffix-length))
         service-id (get-in pod [:metadata :annotations :waiter-service-id])]
     (str service-id \. instance-suffix \- restart-count))))

(defn- killed-by-k8s?
  "Determine whether a pod was killed (restarted) by its corresponding Kubernetes liveness checks."
  [pod-terminated-info]
  ;; TODO (#351) - Look at events for messages about liveness probe failures.
  ;; Currently, we assume any SIGKILL (137) with the default "Error" reason was a livenessProbe kill.
  (and (= 137 (:exitCode pod-terminated-info))
       (= "Error" (:reason pod-terminated-info))))

(defn- track-failed-instances!
  "Update this KubernetesScheduler's service-id->failed-instances-transient-store
   when a new pod failure is listed in the given pod's lastState container status.
   Note that unique instance-ids are deterministically generated each time the pod is restarted
   by passing the pod's restartCount value to the pod->instance-id function."
  [{:keys [service-id] :as live-instance} {:keys [service-id->failed-instances-transient-store]} pod]
  (when-let [newest-failure (get-in pod [:status :containerStatuses 0 :lastState :terminated])]
    (let [failure-flags (if (= "OOMKilled" (:reason newest-failure)) #{:memory-limit-exceeded} #{})
          newest-failure-start-time (-> newest-failure :startedAt timestamp-str->datetime)
          restart-count (get-in pod [:status :containerStatuses 0 :restartCount])
          newest-failure-id (pod->instance-id pod (dec restart-count))
          failures (-> service-id->failed-instances-transient-store deref (get service-id))]
      (when-not (contains? failures newest-failure-id)
        (let [newest-failure-instance (merge live-instance
                                             ;; To match the behavior of the marathon scheduler,
                                             ;; we don't include the exit code in failed instances that were killed by k8s.
                                             (when-not (killed-by-k8s? newest-failure)
                                               {:exit-code (:exitCode newest-failure)})
                                             {:flags failure-flags
                                              :healthy? false
                                              :id newest-failure-id
                                              :started-at newest-failure-start-time})]
          (swap! service-id->failed-instances-transient-store
                 update-in [service-id] assoc newest-failure-id newest-failure-instance))))))

(defn- pod->ServiceInstance
  "Convert a Kubernetes Pod JSON response into a Waiter Service Instance record."
  [pod]
  (try
    (let [port0 (get-in pod [:spec :containers 0 :ports 0 :containerPort])]
      (scheduler/make-ServiceInstance
        {:extra-ports (->> (get-in pod [:metadata :annotations :waiter-port-count])
                           Integer/parseInt range next (mapv #(+ port0 %)))
         :healthy? (get-in pod [:status :containerStatuses 0 :ready] false)
         :host (get-in pod [:status :podIP])
         :id (pod->instance-id pod)
         :log-directory (str "/home/" (get-in pod [:metadata :namespace]))
         :k8s/app-name (get-in pod [:metadata :labels :app])
         :k8s/namespace (get-in pod [:metadata :namespace])
         :k8s/pod-name (get-in pod [:metadata :name])
         :k8s/restart-count (get-in pod [:status :containerStatuses 0 :restartCount])
         :port port0
         :protocol (get-in pod [:metadata :annotations :waiter-protocol])
         :service-id (get-in pod [:metadata :annotations :waiter-service-id])
         :started-at (-> pod
                         (get-in [:status :startTime])
                         (timestamp-str->datetime))}))
    (catch Throwable e
      (log/error e "Error converting pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn api-request
  "Make an HTTP request to the Kubernetes API server using the configured authentication.
   If data is provided via :body, the application/json content type is added automatically.
   The response payload (if any) is automatically parsed to JSON."
  [client url & {:keys [body content-type request-method] :as options}]
  (scheduler/log "Making request to K8s API server:" url request-method body)
  (ss/try+
    (let [auth-str @k8s-api-auth-str
          result (pc/mapply http-utils/http-request client url
                            :accept "application/json"
                            (cond-> options
                              auth-str (assoc-in [:headers "Authorization"] auth-str)
                              (and (not content-type ) body) (assoc :content-type "application/json")))]
      (scheduler/log "Response from K8s API server:" (json/write-str result))
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
  "Get all Waiter Services (reified as ReplicaSets) running in this Kubernetes cluster."
  [{:keys [api-server-url http-client orchestrator-name replicaset-api-version] :as scheduler}]
  (->> (str api-server-url "/apis/" replicaset-api-version
            "/replicasets?labelSelector=managed-by="
             orchestrator-name)
       (api-request http-client)
       :items
       (mapv replicaset->Service)))

(defn- get-replicaset-pods
  "Get all Kubernetes pods associated with the given Waiter Service's corresponding ReplicaSet."
  [{:keys [api-server-url http-client service-id->service-description-fn] :as scheduler}
   {:keys [k8s/app-name k8s/namespace]}]
  (->> (str api-server-url
            "/api/v1/namespaces/"
            namespace
            "/pods?labelSelector=app="
            app-name)
       (api-request http-client)
       :items))

(defn- live-pod?
  "Returns true if the pod has started, but has not yet been deleted."
  [pod]
  (and (some? (get-in pod [:status :podIP]))
       (nil? (get-in pod [:metadata :deletionTimestamp]))))

(defn- get-service-instances
  "Get all Waiter Service Instances associated with the given Waiter Service."
  [{:keys [api-server-url http-client] :as scheduler} basic-service-info]
  (vec (for [pod (get-replicaset-pods scheduler basic-service-info)
             :when (live-pod? pod)]
         (let [service-instance (pod->ServiceInstance pod)]
           (track-failed-instances! service-instance scheduler pod)
           service-instance))))

(defn instances-breakdown
  "Get all Waiter Service Instances associated with the given Waiter Service.
   Grouped by liveness status, i.e.: {:active-instances [...] :failed-instances [...] :killed-instances [...]}"
  [{:keys [service-id->failed-instances-transient-store] :as scheduler} {service-id :id :as basic-service-info}]
  {:active-instances (get-service-instances scheduler basic-service-info)
   :failed-instances (-> @service-id->failed-instances-transient-store (get service-id []) vals vec)
   :killed-instances (-> service-id scheduler/service-id->killed-instances vec)})

(defn- patch-object-json
  "Make a JSON-patch request on a given Kubernetes object."
  [k8s-object-uri http-client ops]
  (api-request http-client k8s-object-uri
               :body (json/write-str ops)
               :content-type "application/json-patch+json"
               :request-method :patch))

(defn- patch-object-replicas
  "Update the replica count in the given Kubernetes object's spec."
  [k8s-object-uri http-client replicas replicas']
  (patch-object-json http-client k8s-object-uri
                     [{:op :test :path "/spec/replicas" :value replicas}
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
                           (with-meta
                             `conflict
                             {:throw-context ~'&throw-context})))]
     (if (not= `conflict patch-result#)
       patch-result#
       (if ~retry-condition
         ~retry-cmd
         (throw (-> patch-result# meta :throw-context :throwable))))))

(defn- build-replicaset-url
  "Build the URL for the given Waiter Service's ReplicaSet."
  [{:keys [api-server-url replicaset-api-version]} {:keys [k8s/app-name k8s/namespace]}]
  (str api-server-url "/apis/" replicaset-api-version
       "/namespaces/" namespace "/replicasets/" app-name))

(defn- scale-service-up-to
  "Scale the number of instances for a given service to a specific number.
   Only used for upward scaling. No-op if it would result in downward scaling."
  [{:keys [http-client max-patch-retries] :as scheduler} service instances']
  (let [replicaset-url (build-replicaset-url scheduler service)]
    (loop [attempt 1
           instances (:instances service)]
      (if (<= instances' instances)
        (log/warn "Skipping non-upward scale-up request on" (:id service)
                  "from" instances "to" instances')
        (k8s-patch-with-retries
          (patch-object-replicas http-client replicaset-url instances instances')
          (<= attempt max-patch-retries)
          (recur (inc attempt) (get-replica-count scheduler replicaset-url)))))))

(defn- scale-service-by-delta
  "Scale the number of instances for a given service by a given delta.
   Can scale either upward (positive delta) or downward (negative delta)."
  [{:keys [http-client max-patch-retries] :as scheduler} service instances-delta]
  (let [replicaset-url (build-replicaset-url scheduler service)]
    (loop [attempt 1
           instances (:instances service)]
      (let [instances' (+ instances instances-delta)]
        (k8s-patch-with-retries
          (patch-object-replicas http-client replicaset-url instances instances')
          (<= attempt max-patch-retries)
          (recur (inc attempt) (get-replica-count scheduler replicaset-url)))))))

(defn- kill-service-instance
  "Safely kill the Kubernetes pod corresponding to the given Waiter Service Instance.
   Returns nil on success, but throws on failure."
  [{:keys [api-server-url http-client] :as scheduler} {:keys [id namespace pod-name service-id] :as instance} service]
  ;; SAFE DELETION STRATEGY:
  ;; 1) Delete the target pod with a grace period of 5 minutes
  ;;    Since the target pod is currently in the "Terminating" state,
  ;;    the owner ReplicaSet will not immediately create a replacement pod.
  ;; 2) Scale down the owner ReplicaSet by 1 pod.
  ;;    Since the target pod is still in the "Terminating" state (assuming delay < 5min),
  ;;    the owner ReplicaSet will not immediately delete a different victim pod.
  ;; 3) Force-delete the target pod. This immediately removes the pod from Kubernetes.
  ;;    The state of the ReplicaSet (desired vs actual pods) should now be consistent.
  ;;    We want to eagerly delete the pod to short-circuit the 5-minute delay from above.
  ;; Note that if it takes more than 5 minutes to get from step 1 to step 2,
  ;; we assume we're already so far out of sync that the possibility of non-atomic scaling
  ;; doesn't hurt us significantly. If it takes more than 5 minutes to get from step 1
  ;; to step 3, then the pod was already deleted, and the force-delete is no longer needed.
  ;; The force-delete can fail with a 404 (object not found), but this operation still succeeds.
  (let [pod-url (str api-server-url
                     "/api/v1/namespaces/"
                     namespace
                     "/pods/"
                     pod-name)
        base-body {:kind "DeleteOptions" :apiVersion "v1"}
        term-json (-> base-body (assoc :gracePeriodSeconds delete-delay-secs) (json/write-str))
        kill-json (-> base-body (assoc :gracePeriodSeconds 0) (json/write-str))
        make-kill-response (fn [killed? message status]
                             {:instance-id id :killed? killed?
                              :message message :service-id service-id :status status})]
    ; request termination of the instance
    (api-request http-client pod-url :request-method :delete :body term-json)
    ; scale down the replicaset to reflect removal of this instance
    (try
      (scale-service-by-delta scheduler service -1)
      (catch Throwable t
        (log/error t "Error while scaling down ReplicaSet after pod termination")))
    ; force-kill the instance (should still be terminating)
    (try
      (api-request http-client pod-url :request-method :delete :body kill-json)
      (catch Throwable t
        (log/error t "Error force-killing pod")))
    ; report back that the instance was killed
    (scheduler/process-instance-killed! instance)
    (comment "Success! Even if the scale-down or force-kill operation failed,
              the pod will be force-killed after the grace period is up.")))

(defn- service-spec
  "Creates a Kubernetes ReplicaSet spec (with an embedded Pod spec) for the given Waiter Service."
  [{:keys [orchestrator-name pod-base-port replicaset-api-version
           replicaset-spec-file-path service-id->password-fn] :as scheduler}
   service-id
   {:strs [backend-proto cmd cpus grace-period-secs health-check-interval-secs
           health-check-max-consecutive-failures mem min-instances ports run-as-user] :as service-description}]
  (let [home-path (str "/home/" run-as-user)
        common-env (scheduler/environment service-id service-description
                                          service-id->password-fn home-path)
        ;; Randomize $PORT0 value to ensure clients can't hardcode it.
        ;; Helps maintain compatibility with Marathon, where port assignment is dynamic.
        port0 (-> (rand-int 100) (* 10) (+ pod-base-port))
        template-env (into [;; We set these two "MESOS_*" variables to improve interoperability.
                            ;; New clients should prefer using WAITER_SANDBOX.
                            {:name "MESOS_DIRECTORY" :value home-path}
                            {:name "MESOS_SANDBOX" :value home-path}]
                           (concat
                             (for [[k v] common-env]
                               {:name k :value v})
                             (for [i (range ports)]
                               {:name (str "PORT" i) :value (str (+ port0 i))})))
        params (into {:k8s-name (service-id->k8s-app-name scheduler service-id)
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
                      :orchestrator-name orchestrator-name
                      :port-count ports
                      :replicaset-api-version replicaset-api-version
                      :run-as-user run-as-user
                      :service-id service-id
                      :ssl? (= "https" backend-proto)}
                     (for [i (range ports)]
                       [(keyword (str "port" i)) (+ port0 i)]))
        edn-opts {:readers {'waiter/param params
                            'waiter/param-str (comp str params)}}]
    (try
      (->> replicaset-spec-file-path
           slurp
           (edn/read-string edn-opts))
      (catch Throwable e
        (log/error e "Error creating ReplicaSet specification for" service-id)
        (throw e)))))

(defn- create-service
  "Reify a Waiter Service as a Kubernetes ReplicaSet."
  [{:keys [service-id] :as descriptor}
   {:keys [api-server-url http-client replicaset-api-version] :as scheduler}]
  (let [{:strs [run-as-user] :as service-description} (:service-description descriptor)
        spec-json (service-spec scheduler service-id service-description)
        request-url (str api-server-url "/apis/" replicaset-api-version "/namespaces/"
                         (service-description->namespace service-description) "/replicasets")
        response-json (api-request http-client request-url
                                   :body (json/write-str spec-json)
                                   :request-method :post)]
    (replicaset->Service response-json)))

(defn- delete-service
  "Delete the Kubernetes ReplicaSet corresponding to a Waiter Service.
   Owned Pods will be removed asynchronously by the Kubernetes garbage collector."
  [{:keys [api-server-url http-client] :as scheduler} {:keys [id] :as service}]
  (let [replicaset-url (build-replicaset-url scheduler service)
        kill-json (json/write-str
                    {:kind "DeleteOptions" :apiVersion "v1"
                     :propagationPolicy "Background"})]
    (api-request http-client replicaset-url :request-method :delete :body kill-json)
    {:message (str "Kubernetes deleted ReplicaSet for " id)
     :result :deleted}))

(defn service-id->service
  "Look up the Kubernetes ReplicaSet associated with a given Waiter service-id,
   and return a corresponding Waiter Service record."
  [{:keys [api-server-url http-client orchestrator-name replicaset-api-version
           service-id->service-description-fn] :as scheduler}
   service-id]
  (when-let [service-ns (-> service-id service-id->service-description-fn service-description->namespace)]
    (some->> (str api-server-url "/apis/" replicaset-api-version
                  "/namespaces/" service-ns
                  "/replicasets?labelSelector=managed-by=" orchestrator-name
                  ",app=" (service-id->k8s-app-name scheduler service-id))
             (api-request http-client)
             :items
             ;; It's possible that multiple Waiter services in different namespaces
             ;; have service-ids mapping to the same Kubernetes object name,
             ;; so we filter to match the full service-id as well.
             (filter #(= service-id (get-in % [:metadata :annotations :waiter-service-id])))
             first
             replicaset->Service)))

; The Waiter Scheduler protocol implementation for Kubernetes
(defrecord KubernetesScheduler [api-server-url http-client
                                max-patch-retries
                                max-name-length
                                orchestrator-name
                                pod-base-port
                                replicaset-api-version
                                replicaset-spec-file-path
                                service-id->failed-instances-transient-store
                                service-id->password-fn
                                service-id->service-description-fn]
  scheduler/ServiceScheduler

  (get-apps->instances [this]
    (pc/map-from-keys #(instances-breakdown this %)
                      (get-services this)))

  (get-apps [this]
    (get-services this))

  (get-instances [this service-id]
    (instances-breakdown this
                         {:id service-id
                          :k8s/app-name (service-id->k8s-app-name this service-id)
                          :k8s/namespace (-> service-id
                                             service-id->service-description-fn
                                             (get "run-as-user"))}))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (ss/try+
      (let [service (service-id->service this service-id)]
        (kill-service-instance this instance service)
        {:instance-id id
         :killed? true
         :message "Successfully killed instance"
         :service-id service-id
         :status 200})
      (catch [:status 404] e
        {:instance-id id
         :killed? false
         :message "Instance not found"
         :service-id service-id
         :status 404})
      (catch Throwable e
        {:instance-id id
         :killed? false
         :message "Error while killing instance"
         :service-id service-id
         :status 500})))

  (app-exists? [this service-id]
    (ss/try+
      (some? (service-id->service this service-id))
      (catch [:status 404] _
        (comment "App does not exist."))))

  (create-app-if-new [this {:keys [service-id] :as descriptor}]
    (when-not (scheduler/app-exists? this service-id)
      (ss/try+
        (create-service descriptor this)
        (catch [:status 409] _
          (log/error "Conflict status when trying to start app. Is app starting up?"
                     descriptor))
        (catch Throwable e
          (log/error e "Error starting new app." descriptor)))))

  (delete-app [this service-id]
    (ss/try+
      (let [service (service-id->service this service-id)
            delete-result (delete-service this service)]
        (swap! service-id->failed-instances-transient-store dissoc service-id)
        (scheduler/remove-killed-instances-for-service! service-id)
        delete-result)
      (catch [:status 404] _
        (log/warn "Service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Kubernetes reports service does not exist"})
      (catch Throwable e
        (log/warn "Internal error while deleting service"
                  {:service-id service-id})
        {:result :error
         :message "Internal error while deleting service"})))

  (scale-app [this service-id scale-to-instances _]
    (ss/try+
      (if-let [service (service-id->service this service-id)]
        (do
          (scale-service-up-to this service scale-to-instances)
          {:success true
           :status 200
           :result :scaled
           :message (str "Scaled to " scale-to-instances)})
        (do
          (log/error "Cannot scale missing service" service-id)
          {:success false
           :status 404
           :result :no-such-service-exists
           :message "Failed to scale missing service"}))
      (catch [:status 409] _
        {:success false
         :status 409
         :result :conflict
         :message "Scaling failed due to repeated patch conflicts"})
      (catch Throwable e
        (log/error e "Error while scaling waiter service" service-id)
        {:success false
         :status 500
         :result :failed
         :message "Error while scaling waiter service"})))

  (retrieve-directory-content [_ service-id instance-id _ relative-directory]
    ;; TODO (#357) - get access to the working directory contents in the pod
    [])

  (service-id->state [_ service-id]
    {:failed-instances (vals (get @service-id->failed-instances-transient-store service-id))
     :killed-instances (scheduler/service-id->killed-instances service-id)})

  (state [_]
    {:service-id->failed-instances @service-id->failed-instances-transient-store}))

(defn- start-auth-renewer
  "Initialize the k8s-api-auth-str atom,
   and optionally start a chime to periodically referesh the value."
  [{:keys [refresh-delay-mins refresh-fn]}]
  {:pre [(or (nil? refresh-delay-mins)
             (utils/pos-int? refresh-delay-mins))
         (fn? refresh-fn)]}
  (let [refresh (-> refresh-fn utils/resolve-symbol deref)
        auth-update-fn (fn auth-update []
                         (if-let [auth-str' (refresh)]
                           (reset! k8s-api-auth-str auth-str')))]
    (auth-update-fn)
    (when [refresh-delay-mins]
      (du/start-timer-task
        (t/minutes refresh-delay-mins)
        auth-update-fn
        :delay-ms (* 60000 refresh-delay-mins)))))

(defn kubernetes-scheduler
  "Returns a new KubernetesScheduler with the provided configuration. Validates the
   configuration against kubernetes-scheduler-schema and throws if it's not valid."
  [{:keys [authentication http-options max-patch-retries max-name-length
           orchestrator-name pod-base-port replicaset-api-version
           replicaset-spec-file-path service-id->service-description-fn
           service-id->password-fn url]}]
  {:pre [(utils/pos-int? (:socket-timeout http-options))
         (utils/pos-int? (:conn-timeout http-options))
         (utils/non-neg-int? max-patch-retries)
         (utils/pos-int? max-name-length)
         (not (string/blank? orchestrator-name))
         (integer? pod-base-port)
         (< 0 pod-base-port 65527)  ; max port is 65535, and we need to reserve up to 10 ports
         (not (string/blank? replicaset-api-version))
         (some-> replicaset-spec-file-path io/as-file .exists)
         (some? (io/as-url url))]}
  (let [http-client (http-utils/http-client-factory http-options)
        service-id->failed-instances-transient-store (atom {})]
    (when authentication
      (start-auth-renewer authentication))
    (->KubernetesScheduler url http-client
                           max-patch-retries
                           max-name-length
                           orchestrator-name
                           pod-base-port
                           replicaset-api-version
                           replicaset-spec-file-path
                           service-id->failed-instances-transient-store
                           service-id->password-fn
                           service-id->service-description-fn)))
