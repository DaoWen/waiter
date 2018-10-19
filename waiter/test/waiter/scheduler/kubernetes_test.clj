;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.scheduler.kubernetes-test
  (:require [clojure.core.async :as async]
            [clojure.data]
            [clojure.pprint]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.kubernetes :refer :all]
            [waiter.util.client-tools :as ct]
            [waiter.util.http-utils :as hu]
            [waiter.util.date-utils :as du])
  (:import (waiter.scheduler Service ServiceInstance)
           (waiter.scheduler.kubernetes KubernetesScheduler)))

(defmacro throw-exception
  "Throw a RuntimeException with the stack trace suppressed.
   Suppressing the stack trace helps keep our test logs cleaner."
  []
  `(throw
     (doto (RuntimeException.)
       (.setStackTrace (make-array StackTraceElement 0)))))

(def ^:const default-pod-suffix-length 5)

(defn- make-dummy-scheduler
  ([service-ids] (make-dummy-scheduler service-ids {}))
  ([service-ids args]
   (->
     {:authorizer {:kind :default
                   :default {:factory-fn 'waiter.authorization/noop-authorizer}}
      :watch-state (atom nil)
      :fileserver {:port 9090
                   :scheme "http"}
      :max-patch-retries 5
      :max-name-length 63
      :orchestrator-name "waiter"
      :pod-base-port 8080
      :pod-suffix-length default-pod-suffix-length
      :replicaset-api-version "extensions/v1beta1"
      :replicaset-spec-builder-fn #(waiter.scheduler.kubernetes/default-replicaset-builder
                                     %1 %2 %3
                                     {:default-container-image "twosigma/kitchen:latest"})
      :service-id->failed-instances-transient-store (atom {})
      :service-id->password-fn #(str "password-" %)
      :service-id->service-description-fn (pc/map-from-keys (constantly {"run-as-user" "myself"})
                                                            service-ids)}
     (merge args)
     map->KubernetesScheduler)))

(def dummy-service-description
  {"backend-proto" "HTTP"
   "cmd" "foo"
   "cpus" 1.2
   "grace-period-secs" 7
   "health-check-interval-secs" 10
   "health-check-max-consecutive-failures" 2
   "mem" 1024
   "min-instances" 1
   "ports" 2
   "run-as-user" "myself"})

(defn- sanitize-k8s-service-records
  "Walks data structure to remove extra fields added by Kubernetes from Service and ServiceInstance records."
  [walkable-collection]
  (walk/postwalk
    (fn sanitizer [x]
      (cond
        (instance? Service x)
        (dissoc x :k8s/app-name :k8s/namespace)
        (instance? ServiceInstance x)
        (dissoc x :k8s/app-name :k8s/namespace :k8s/pod-name :k8s/restart-count)
        :else x))
    walkable-collection))

(defmacro assert-data-equal
  [expected actual]
  `(let [expected# ~expected
         actual# ~actual]
     (when-not (= expected# actual#)
       (clojure.pprint/pprint
         (clojure.data/diff expected# actual#)))
     (is (= expected# actual#))))

(deftest test-service-id->k8s-app-name
  (let [base-scheduler-spec {:pod-suffix-length default-pod-suffix-length}
        long-app-name (apply str (repeat 200 \A))
        sample-uuid "e8b625cc83c411e8974c38d5474b213d"
        short-app-name "myapp"
        short-sample-uuid (str (subs sample-uuid 0 8)
                               (subs sample-uuid 24))
        waiter-service-prefix "waiter-service-id-prefix-"]
    (let [{:keys [max-name-length] :as long-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 128)]
      (testing "Very long name length limit maps long service-id through unmodified"
        (let [service-id (str "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= service-id (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps long service-id through without prefix"
        (let [service-id (str waiter-service-prefix
                              "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= (subs service-id (count waiter-service-prefix))
                 (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps truncates service name, but keeps full uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 32 1 default-pod-suffix-length))
                      "-" sample-uuid)
                 (service-id->k8s-app-name long-name-scheduler service-id))))))
    (let [{:keys [max-name-length] :as short-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 32)]
      (assert (== 32 (count sample-uuid)))
      (assert (== 16 (count short-sample-uuid)))
      (testing "Short name length limit with short app name only shortens uuid"
        (let [service-id (str short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with short app name only removes prefix, shortens uuid"
        (let [service-id (str waiter-service-prefix short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with long app name truncates app name, removes prefix, and shortens uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 16 1 default-pod-suffix-length))
                      "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id))))))))

(defn- reset-scheduler-watch-state!
  ([scheduler rs-response]
   (reset-scheduler-watch-state! scheduler rs-response nil))
  ([{:keys [watch-state] :as scheduler} rs-response pods-response]
   (let [dummy-resource-uri "http://ignored-uri"
         rs-state (with-redefs [api-request (constantly rs-response)]
                    (global-rs-state-query scheduler dummy-resource-uri))
         pods-state (with-redefs [api-request (constantly pods-response)]
                      (global-pods-state-query scheduler dummy-resource-uri))
         global-state (merge rs-state pods-state)]
     (reset! watch-state global-state))))

(deftest test-scheduler-get-services
  (let [test-cases
        [{:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items []}
          :expected-result nil}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-1234"
                               :namespace "myself"
                               :labels {:app "test-app-1234"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-1234"}}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-1234"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:metadata {:name "test-app-6789"
                               :namespace "myself"
                               :labels {:app "test-app-6789"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-6789"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-6789"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-1234"
                                    :instances 2
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-6789" :instances 3 :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}
         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-abcd"
                               :namespace "myself"
                               :labels {:app "test-app-abcd"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-abcd"}}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-abcd"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:mismatched-replicaset "should be ignored"}
                   {:metadata {:name "test-app-wxyz"
                               :namespace "myself"
                               :labels {:app "test-app-wxyz"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-wxyz"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-wxyz"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-abcd"
                                    :instances 2
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-wxyz" :instances 3 :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-4321"
                               :namespace "myself"
                               :labels {:app "test-app-4321"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-4321"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-4321"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 1
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-4321" :instances 3 :task-count 3
                                    :task-stats {:running 2 :healthy 1 :unhealthy 1 :staged 1}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-9999"
                               :namespace "myself"
                               :labels {:app "test-app-9999"
                                        :managed-by "waiter"}
                               :annotations {:waiter/service-id "test-app-9999"}}
                    :spec {:replicas 0
                           :selector {:matchLabels {:app "test-app-9999"
                                                    :managed-by "waiter"}}}
                    :status {:replicas 0
                             :readyReplicas 0
                             :availableReplicas 0}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-9999"
                                    :instances 0
                                    :task-count 0
                                    :task-stats {:running 0, :healthy 0, :unhealthy 0, :staged 0}})]}]]
    (doseq [{:keys [api-server-response expected-result]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            actual-result (->> dummy-scheduler
                               scheduler/get-services
                               sanitize-k8s-service-records)]
        (assert-data-equal expected-result actual-result)))))

(deftest test-scheduler-get-service->instances
  (let [rs-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :managed-by "waiter"}
                             :annotations {:waiter/service-id "test-app-1234"}}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app "test-app-1234"
                                                  :managed-by "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}
                 {:metadata {:name "test-app-6789"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :managed-by "waiter"}
                             :annotations {:waiter/service-id "test-app-6789"}}
                  :spec {:replicas 3
                         :selector {:matchLabels {:app "test-app-6789"
                                                  :managed-by "waiter"}}}
                  :status {:replicas 3
                           :readyReplicas 1
                           :availableReplicas 2
                           :unavailableReplicas 1}}]}

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :items [{:metadata {:name "test-app-1234-abcd1"
                       :namespace "myself"
                       :labels {:app "test-app-1234"
                                :managed-by "waiter"}
                       :annotations {:waiter/port-count "1"
                                     :waiter/protocol "https"
                                     :waiter/service-id "test-app-1234"}}
            :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
            :status {:podIP "10.141.141.11"
                     :startTime "2014-09-13T00:24:46Z"
                     :containerStatuses [{:name "test-app-1234"
                                          :ready true
                                          :restartCount 0}]}}
           {:metadata {:name "test-app-1234-abcd2"
                       :namespace "myself"
                       :labels {:app "test-app-1234"
                                :managed-by "waiter"}
                       :annotations {:waiter/port-count "1"
                                     :waiter/protocol "https"
                                     :waiter/service-id "test-app-1234"}}
            :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
            :status {:podIP "10.141.141.12"
                     :startTime "2014-09-13T00:24:47Z"
                     :containerStatuses [{:name "test-app-1234"
                                          :ready true
                                          :restartCount 0}]}}
           {:metadata {:name "test-app-6789-abcd1"
                       :namespace "myself"
                       :labels {:app "test-app-6789"
                                :managed-by "waiter"}
                       :annotations {:waiter/port-count "1"
                                     :waiter/protocol "http"
                                     :waiter/service-id "test-app-6789"}}
            :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
            :status {:podIP "10.141.141.13"
                     :startTime "2014-09-13T00:24:35Z"
                     :containerStatuses [{:name "test-app-6789"
                                          :ready true
                                          :restartCount 0}]}}
           {:metadata {:name "test-app-6789-abcd2"
                       :namespace "myself"
                       :labels {:app "test-app-6789"
                                :managed-by "waiter"}
                       :annotations {:waiter/port-count "1"
                                     :waiter/protocol "http"
                                     :waiter/service-id "test-app-6789"}}
            :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
            :status {:podIP "10.141.141.14"
                     :startTime "2014-09-13T00:24:37Z"
                     :containerStatuses [{:name "test-app-6789"
                                          :lastState {:terminated {:exitCode 255
                                                                   :reason "Error"
                                                                   :startedAt "2014-09-13T00:24:36Z"}}
                                          :restartCount 1}]}}
           {:metadata {:name "test-app-6789-abcd3"
                       :namespace "myself"
                       :labels {:app "test-app-6789"
                                :managed-by "waiter"}
                       :annotations {:waiter/port-count "1"
                                     :waiter/protocol "http"
                                     :waiter/service-id "test-app-6789"}}
            :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
            :status {:podIP "10.141.141.15"
                     :startTime "2014-09-13T00:24:38Z"
                     :containerStatuses [{:name "test-app-6789"
                                          :restartCount 0}]}}]}

        expected (hash-map
                   (scheduler/make-Service {:id "test-app-1234"
                                            :instances 2
                                            :task-count 2
                                            :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.11"
                        :id "test-app-1234.test-app-1234-abcd1-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "https"
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.12"
                        :id "test-app-1234.test-app-1234-abcd2-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "https"
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:47Z" k8s-timestamp-format)})]
                    :failed-instances []}

                   (scheduler/make-Service {:id "test-app-6789" :instances 3 :task-count 3
                                            :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.13"
                        :id "test-app-6789.test-app-6789-abcd1-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:35Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.14"
                        :id "test-app-6789.test-app-6789-abcd2-1"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:37Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.15"
                        :id "test-app-6789.test-app-6789-abcd3-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:38Z" k8s-timestamp-format)})]
                    :failed-instances
                    [(scheduler/make-ServiceInstance
                       {:exit-code 255
                        :healthy? false
                        :host "10.141.141.14"
                        :id "test-app-6789.test-app-6789-abcd2-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:36Z" k8s-timestamp-format)})]})
        dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
        _ (reset-scheduler-watch-state! dummy-scheduler rs-response pods-response)
        actual (->> dummy-scheduler
                    get-service->instances
                    sanitize-k8s-service-records)]
    (assert-data-equal expected actual)))

(deftest test-kill-instance
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/namespace "myself"})
        instance-id "instance-id"
        instance (scheduler/make-ServiceInstance
                   {:extra-ports []
                    :healthy? true
                    :host "10.141.141.10"
                    :id instance-id
                    :log-directory "/home/myself"
                    :k8s/namespace "myself"
                    :port 8080
                    :protocol "https"
                    :service-id service-id
                    :started-at (du/str-to-date "2014-09-13T00:24:56Z" k8s-timestamp-format)})
        dummy-scheduler (make-dummy-scheduler [service-id])
        partial-expected {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :killed? true
                   :message "Successfully killed instance"
                   :status 200)
                 actual))))
      (testing "unsuccessful-delete: no such instance"
        (let [error-msg "Instance not found"
              actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :message error-msg
                   :status 404)
                 actual))))
      (testing "successful-delete: terminated, but had patch conflict"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (if (= request-method :patch)
                                                   (ss/throw+ {:status 409})
                                                   {:spec {:replicas 1}}))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :killed? true
                   :message "Successfully killed instance"
                   :status 200)
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :message "Error while killing instance"
                   :status 500)
                 actual)))))))

(deftest test-scheduler-service-exists?
  (let [service-id "test-app-1234"
        empty-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items []}
        non-empty-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items [{:metadata {:name service-id
                             :namespace "myself"
                             :labels {:app service-id
                                      :managed-by "waiter"}
                             :annotations {:waiter/service-id service-id}}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app service-id
                                                  :managed-by "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}]}
        test-cases [{:api-server-response nil
                     :expected-result false}
                    {:api-server-response {}
                     :expected-result false}
                    {:api-server-response empty-response
                     :expected-result false}
                    {:api-server-response non-empty-response
                     :expected-result true}]]
    (doseq [{:keys [api-server-response expected-result]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler [service-id])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            actual-result (scheduler/service-exists? dummy-scheduler service-id)]
        (is (= expected-result actual-result))))))

(deftest test-create-app
  (let [service-id "test-service-id"
        service {:service-id service-id}
        descriptor {:service-description dummy-service-description
                    :service-id service-id}
        dummy-scheduler (make-dummy-scheduler [service-id])]
    (testing "unsuccessful-create: app already exists"
      (let [actual (with-redefs [service-id->service (constantly service)]
                     (scheduler/create-service-if-new dummy-scheduler descriptor))]
        (is (nil? actual))))
    (with-redefs [service-id->service (constantly nil)]
      (testing "unsuccessful-create: service creation conflict (already running)"
        (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                 (ss/throw+ {:status 409}))]
                       (scheduler/create-service-if-new dummy-scheduler descriptor))]
          (is (nil? actual))))
      (testing "unsuccessful-create: internal error"
        (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                 (throw-exception))]
                       (scheduler/create-service-if-new dummy-scheduler descriptor))]
          (is (nil? actual))))
      (testing "successful create"
        (let [actual (with-redefs [api-request (constantly service)
                                   replicaset->Service identity]
                       (scheduler/create-service-if-new dummy-scheduler descriptor))]
          (is (= service actual)))))))

(deftest test-keywords-in-replicaset-spec
  (testing "namespaced keywords in annotation keys and values correctly converted"
    (let [service-id "test-service-id"
          descriptor {:service-description dummy-service-description
                      :service-id service-id}
          dummy-scheduler (-> (make-dummy-scheduler [service-id])
                              (update :replicaset-spec-builder-fn
                                      (fn [base-spec-builder-fn]
                                        (fn [scheduler service-id context]
                                          (-> (base-spec-builder-fn scheduler service-id context)
                                              (assoc-in [:metadata :annotations] {:waiter/x :waiter/y}))))))]
      (let [spec-json (with-redefs [api-request (fn [_ _ & {:keys [body]}] body)
                                    replicaset->Service identity]
                        (create-service descriptor dummy-scheduler))]
        (is (string/includes? spec-json "\"annotations\":{\"waiter/x\":\"waiter/y\"}"))))))

(deftest test-delete-service
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        dummy-scheduler (make-dummy-scheduler [service-id])]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message (str "Kubernetes deleted ReplicaSet for " service-id)
                  :result :deleted}
                 actual))))
      (testing "unsuccessful-delete: service not found"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Kubernetes reports service does not exist"
                  :result :no-such-service-exists}
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Internal error while deleting service"
                  :result :error}
                 actual)))))))

(deftest test-scale-service
  (let [instances' 4
        service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        service-state (atom {:services {service-id service}})
        dummy-scheduler (-> (make-dummy-scheduler [service-id])
                            (assoc :watch-state service-state))]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-scale"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success true
                  :status 200
                  :result :scaled
                  :message (str "Scaled to " instances')}
                 actual))))
      (testing "unsuccessful-scale: service not found"
        (let [actual (with-redefs [service-id->service (constantly nil)]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 404
                  :result :no-such-service-exists
                  :message "Failed to scale missing service"}
                 actual))))
      (testing "unsuccessful-scale: patch conflict"
        (let [actual (with-redefs [api-request (fn [& _] (ss/throw+ {:status 409}))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 409
                  :result :conflict
                  :message "Scaling failed due to repeated patch conflicts"}
                 actual))))
      (testing "unsuccessful-scale: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 500
                  :result :failed
                  :message "Error while scaling waiter service"}
                 actual)))))))

(deftest test-retrieve-directory-content
  (let [service-id "test-service-id"
        instance-id "test-service-instance-id"
        host "host.local"
        path "/some/path/"
        dummy-scheduler (make-dummy-scheduler [service-id])
        port (get-in dummy-scheduler [:fileserver :port])
        make-file (fn [file-name size]
                    {:url (str "http://" host ":" port path file-name)
                     :name file-name
                     :size 1
                     :type "file"})
        make-dir (fn [dir-name]
                   {:path (str path dir-name)
                    :name dir-name
                    :type "directory"})
        strip-links (partial mapv #(dissoc % :url))
        inputs [{:description "empty directory"
                 :expected []}
                {:description "single file"
                 :expected [(make-file "foo" 1)]}
                {:description "single directory"
                 :expected [(make-dir "foo")]}
                {:description "lots of stuff"
                 :expected [(make-file "a" 10)
                            (make-file "b" 30)
                            (make-file "c" 40)
                            (make-file "d" 50)
                            (make-dir "w")
                            (make-dir "x")
                            (make-dir "y")
                            (make-dir "z")]}]]
    (doseq [{:keys [description expected]} inputs]
      (testing description
        (let [actual (with-redefs [hu/http-request (constantly (strip-links expected))]
                       (scheduler/retrieve-directory-content
                         dummy-scheduler service-id instance-id host path))]
          (is (= expected actual)))))))

(defn test-auth-refresher
  "Test implementation of the authentication action-fn"
  [{:keys [refresh-value] :as context}]
  refresh-value)

(deftest test-kubernetes-scheduler
  (let [context {:is-waiter-service?-fn (constantly nil)
                 :leader?-fn (constantly nil)
                 :scheduler-name "kubernetes"
                 :scheduler-state-chan (async/chan 4)
                 :scheduler-syncer-interval-secs 5
                 :service-id->password-fn (constantly nil)
                 :service-id->service-description-fn (constantly nil)
                 :start-scheduler-syncer-fn (constantly nil)}
        k8s-config {:authentication nil
                    :authorizer {:kind :default
                                 :default {:factory-fn 'waiter.authorization/noop-authorizer}}
                    :fileserver {:port 9090
                                 :scheme "http"}
                    :watch-state (atom nil)
                    :http-options {:conn-timeout 10000
                                   :socket-timeout 10000}
                    :max-patch-retries 5
                    :max-name-length 63
                    :orchestrator-name "waiter"
                    :pod-base-port 8080
                    :pod-suffix-length default-pod-suffix-length
                    :replicaset-api-version "extensions/v1beta1"
                    :replicaset-spec-builder {:factory-fn 'waiter.scheduler.kubernetes/default-replicaset-builder
                                              :default-container-image "twosigma/kitchen:latest"}
                    :url "http://127.0.0.1:8001"}
        base-config (merge context k8s-config)]
    (with-redefs [start-k8s-watch! (constantly nil)]
      (testing "Creating a KubernetesScheduler"

        (testing "should throw on invalid configuration"
          (testing "bad url"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url ""))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url "localhost")))))
          (testing "bad http options"
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :conn-timeout] 0))))
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :socket-timeout] 0)))))
          (testing "bad max conflict retries"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-patch-retries -1)))))
          (testing "bad max name length"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-name-length 0)))))
          (testing "bad ReplicaSet spec factory function"
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] "not a symbol"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] :not-a-symbol))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] 'not.a.namespace/not-a-fn)))))
          (testing "bad base port number"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port -1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port "8080"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port 1234567890))))))

        (testing "should work with valid configuration"
          (is (instance? KubernetesScheduler (kubernetes-scheduler base-config))))

        (testing "periodic auth-refresh task"
          (let [kill-task-fn (atom (constantly nil))
                orig-start-auth-renewer start-auth-renewer
                secret-value "secret-value"]
            (try
              (with-redefs [start-auth-renewer #(reset! kill-task-fn (apply orig-start-auth-renewer %&))]
                (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :authentication {:action-fn `test-auth-refresher
                                                                                                             :refresh-delay-mins 1
                                                                                                             :refresh-value secret-value})))))
              (is (ct/wait-for #(= secret-value @k8s-api-auth-str) :interval 1 :timeout 10))
              (finally
                (@kill-task-fn)))))))))
