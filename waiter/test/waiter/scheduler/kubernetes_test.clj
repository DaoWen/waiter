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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data]
            [clojure.data.json :as json]
            [clojure.pprint]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.scheduler.kubernetes :refer :all]
            [waiter.scheduler :as scheduler]
            [waiter.util.date-utils :as du])
  (:import waiter.scheduler.kubernetes.KubernetesScheduler
           (waiter.scheduler Service ServiceInstance)))

(defn- make-dummy-scheduler
  ([service-ids] (make-dummy-scheduler service-ids {}))
  ([service-ids args]
   (->
     {:max-conflict-retries 5
      :max-name-length 63
      :service-id->failed-instances-transient-store (atom {})
      :service-id->service-description-fn (pc/map-from-keys (constantly {"run-as-user" "myself"})
                                                            service-ids)}
     (merge args)
     map->KubernetesScheduler)))

(defn- sanitize-k8s-service-records
  "Walks data structure to remove extra fields added by Kubernetes from Service and ServiceInstance records."
  [walkable-collection]
  (walk/postwalk
    (fn sanitizer [x]
      (cond
        (instance? Service x)
        (dissoc x :k8s-name :namespace)
        (instance? ServiceInstance x)
        (dissoc x :k8s-name :namespace :pod-name :restart-count)
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

(deftest test-scheduler-get-instances
  (let [test-cases [{:name "get-instances no response"
                     :kubernetes-response nil
                     :expected-response {:active-instances []
                                         :failed-instances []
                                         :killed-instances []}}

                    {:name "get-instances empty response"
                     :kubernetes-response {}
                     :expected-response {:active-instances []
                                         :failed-instances []
                                         :killed-instances []}}

                    {:name "get-instances empty-app response"
                     :kubernetes-response {:apiVersion "v1" :items [] :kind "List"
                                           :metadata {:resourceVersion "" :selfLink ""}}
                     :expected-response {:active-instances []
                                         :failed-instances []
                                         :killed-instances []}}

                    {:name "get-instances valid response with task failure"
                     :kubernetes-response
                     {:apiVersion "v1"
                      :kind "List"
                      :metadata {}
                      :items [{:metadata {:annotations {:waiter/port-count "1"
                                                        :waiter/protocol "https"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd1"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:ready true
                                                             :restartCount 0}]
                                        :podIP "10.141.141.10"
                                        :startTime "2014-09-13T00:24:46Z"}}
                              {:metadata {:annotations {:waiter/port-count "1"
                                                        :waiter/protocol "https"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd2"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:lastState {:terminated {:exitCode 1
                                                                                      :startedAt "2014-09-12T23:23:41Z"}}
                                                             :ready true
                                                             :restartCount 1}]
                                        :podIP "10.141.141.11"
                                        :startTime "2014-09-13T00:24:56Z"}}
                              {:metadata {:annotations {:waiter/port-count "4"
                                                        :waiter/protocol "https"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd3"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:ready false
                                                             :restartCount 0}]
                                        :podIP "10.141.141.12"
                                        :startTime "2014-09-14T00:24:46Z"}}]}

                     :expected-response
                     {:active-instances [(scheduler/make-ServiceInstance
                                           {:extra-ports [],
                                            :healthy? true,
                                            :host "10.141.141.10",
                                            :id "test-app-1234.abcd1-0",
                                            :log-directory "/home/myself"
                                            :port 8080,
                                            :protocol "https",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)}),
                                         (scheduler/make-ServiceInstance
                                           {:extra-ports [],
                                            :healthy? true,
                                            :host "10.141.141.11",
                                            :id "test-app-1234.abcd2-1",
                                            :log-directory "/home/myself"
                                            :port 8080,
                                            :protocol "https",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-13T00:24:56Z" k8s-timestamp-format)}),
                                         (scheduler/make-ServiceInstance
                                           {:extra-ports [8081 8082 8083],
                                            :healthy? false,
                                            :host "10.141.141.12",
                                            :id "test-app-1234.abcd3-0",
                                            :log-directory "/home/myself"
                                            :port 8080,
                                            :protocol "https",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-14T00:24:46Z" k8s-timestamp-format)})]
                      :failed-instances [(scheduler/make-ServiceInstance
                                           {:exit-code 1
                                            :extra-ports [],
                                            :healthy? false,
                                            :host "10.141.141.11",
                                            :id "test-app-1234.abcd2-0",
                                            :log-directory "/home/myself"
                                            :port 8080,
                                            :protocol "https",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-12T23:23:41Z" k8s-timestamp-format)})]
                      :killed-instances []}}

                    {:name "get-instances valid response without task failure"
                     :kubernetes-response
                     {:apiVersion "v1"
                      :kind "List"
                      :metadata {}
                      :items [{:metadata {:annotations {:waiter/port-count "1"
                                                        :waiter/protocol "http"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd1"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:ready true
                                                             :restartCount 0}]
                                        :podIP "10.141.141.11"
                                        :startTime "2014-09-13T00:24:46Z"}}
                              {:metadata {:annotations {:waiter/port-count "1"
                                                        :waiter/protocol "http"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd2"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:ready true
                                                             :restartCount 0}]
                                        :podIP "10.141.141.12"
                                        :startTime "2014-09-13T00:24:47Z"}}
                              {:metadata {:annotations {:waiter/port-count "1"
                                                        :waiter/protocol "http"
                                                        :waiter/service-id "test-app-1234"}
                                          :labels {:app "test-app-1234"
                                                   :managed-by "waiter"}
                                          :name "test-app-1234-abcd3"
                                          :namespace "myself"}
                               :spec {:containers [{:name "test-app-1234"
                                                    :ports [{:containerPort 8080}]}]}
                               :status {:containerStatuses [{:ready false
                                                             :restartCount 0}]
                                        :podIP "10.141.141.13"
                                        :startTime "2014-09-14T00:24:48Z"}}]}

                     :expected-response
                     {:active-instances [(scheduler/make-ServiceInstance
                                           {:extra-ports [],
                                            :healthy? true,
                                            :host "10.141.141.11",
                                            :id "test-app-1234.abcd1-0",
                                            :log-directory "/home/myself"
                                            :port 8080,
                                            :protocol "http",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)}),
                                         (scheduler/make-ServiceInstance
                                           {:extra-ports [],
                                            :healthy? true,
                                            :host "10.141.141.12",
                                            :log-directory "/home/myself"
                                            :id "test-app-1234.abcd2-0",
                                            :port 8080,
                                            :protocol "http",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-13T00:24:47Z" k8s-timestamp-format)}),
                                         (scheduler/make-ServiceInstance
                                           {:extra-ports [],
                                            :healthy? false,
                                            :host "10.141.141.13",
                                            :log-directory "/home/myself"
                                            :id "test-app-1234.abcd3-0",
                                            :port 8080,
                                            :protocol "http",
                                            :service-id "test-app-1234",
                                            :started-at (du/str-to-date "2014-09-14T00:24:48Z" k8s-timestamp-format)})]
                      :failed-instances []
                      :killed-instances []}}]]
    (doseq [{:keys [expected-response kubernetes-response name]} test-cases]
      (testing (str "Test " name)
        (let [service-id "test-app-1234"
              dummy-scheduler (make-dummy-scheduler [service-id])
              actual-response (with-redefs [;; mock the K8s API server returning our test responses
                                            api-request (constantly kubernetes-response)]
                                (->> (scheduler/get-instances dummy-scheduler service-id)
                                     sanitize-k8s-service-records))]
          (is (= expected-response actual-response) (str name))
          (is (== (-> expected-response :failed-instances count)
                  (-> dummy-scheduler :service-id->failed-instances-transient-store deref count))
              (str name))
          (scheduler/preserve-only-killed-instances-for-services! []))))))

(deftest test-scheduler-get-apps
  (let [test-cases
        [{:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items []}
          :expected-result []}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-1234"
                               :namespace "myself"
                               :labels {:app "test-app-1234"
                                        :managed-by "waiter"}
                               :annotations {:waiter/app-status "live"
                                             :waiter/service-id "test-app-1234"}}
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
                               :annotations {:waiter/app-status "live"
                                             :waiter/service-id "test-app-6789"}}
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
           :items [{:metadata {:name "test-app-9999"
                               :namespace "myself"
                               :labels {:app "test-app-9999"
                                        :managed-by "waiter"}
                               :annotations {:waiter/app-status "killed"
                                             :waiter/service-id "test-app-9999"}}
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
            actual-result (with-redefs [api-request (constantly api-server-response)]
                            (->> dummy-scheduler
                                 scheduler/get-apps
                                 sanitize-k8s-service-records))]
        (assert-data-equal expected-result actual-result)))))

(deftest test-scheduler-get-apps->instances
  (let [services-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :managed-by "waiter"}
                             :annotations {:waiter/app-status "live"
                                           :waiter/service-id "test-app-1234"}}
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
                             :annotations {:waiter/app-status "live"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:replicas 3
                         :selector {:matchLabels {:app "test-app-6789"
                                                  :managed-by "waiter"}}}
                  :status {:replicas 3
                           :readyReplicas 1
                           :availableReplicas 2
                           :unavailableReplicas 1}}]}

        app-1234-pods-response
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
                           :startTime  "2014-09-13T00:24:46Z"
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
                                                :restartCount 0}]}}]}

        app-6789-pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :items [{:metadata {:name "test-app-6789-abcd1"
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

        api-server-responses [services-response app-1234-pods-response app-6789-pods-response]

        expected (hash-map
                   (scheduler/make-Service {:id "test-app-1234"
                                            :instances 2
                                            :task-count 2
                                            :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.11"
                        :id "test-app-1234.abcd1-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "https"
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.12"
                        :id "test-app-1234.abcd2-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "https"
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:47Z" k8s-timestamp-format)})]
                    :failed-instances []
                    :killed-instances []}

                   (scheduler/make-Service {:id "test-app-6789" :instances 3 :task-count 3
                                            :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.13"
                        :id "test-app-6789.abcd1-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:35Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.14"
                        :id "test-app-6789.abcd2-1"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:37Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.15"
                        :id "test-app-6789.abcd3-0"
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
                        :id "test-app-6789.abcd2-0"
                        :log-directory "/home/myself"
                        :port 8080
                        :protocol "http"
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:36Z" k8s-timestamp-format)})]
                    :killed-instances []})
        dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
        response-iterator (.iterator api-server-responses)
        actual (with-redefs [api-request (fn [& _] (.next response-iterator))]
                 (->> dummy-scheduler
                      scheduler/get-apps->instances
                      sanitize-k8s-service-records))]
    (assert-data-equal expected actual)
    (scheduler/preserve-only-killed-instances-for-services! [])))

(deftest test-kill-instance
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :namespace "myself"})
        instance-id "instance-id"
        instance (scheduler/make-ServiceInstance
                   {:extra-ports []
                    :healthy? true
                    :host "10.141.141.10"
                    :id instance-id
                    :log-directory "/home/myself"
                    :namespace "myself"
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
              actual (with-redefs [api-request (fn mocked-api-request [_ url & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message error-msg
                        :status 404)
                 actual))))
      (testing "unsuccessful-delete: patch conflict"
        (let [error-msg "Failed to update service specification due to repeated conflicts"
              actual (with-redefs [api-request (fn mocked-api-request [_ url & {:keys [request-method]}]
                                                 (if (= request-method :patch)
                                                   (ss/throw+ {:status 409})
                                                   {:spec {:replicas 1}}))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message error-msg
                        :status 409)
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [error-msg "Unable to kill instance"
              actual (with-redefs [api-request (fn [& _] (throw (RuntimeException. error-msg)))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message error-msg
                        :status 500)
                 actual)))))))

(deftest test-scheduler-app-exists?
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
                             :annotations {:waiter/app-status "live"
                                           :waiter/service-id service-id}}
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
            actual-result (with-redefs [api-request (constantly api-server-response)]
                            (scheduler/app-exists? dummy-scheduler service-id))]
        (is (= expected-result actual-result))))))

(deftest test-killed-instances-transient-store
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        kubernetes-api (Object.)
        dummy-scheduler (make-dummy-scheduler ["service-1" "service-2" "service-3"])
        make-instance (fn [service-id instance-suffix]
                        (let [instance-id (str service-id \. instance-suffix)]
                          {:id instance-id
                           :namespace (-> dummy-scheduler
                                          :service-id->service-description-fn
                                          (get service-id)
                                          (get "run-as-user"))
                           :pod-name (str instance-id "-0")
                           :service-id service-id}))
        make-killed-instance (fn [service-id instance-suffix]
                               (assoc (make-instance service-id instance-suffix)
                                      :killed-at current-time-str))]
    (with-redefs [api-request (constantly {:status "OK"})
                  service-id->service (fn service-id->dummy-service [_ service-id]
                                        (scheduler/make-Service
                                          {:id service-id :instances 1 :namespace "myself"}))
                  t/now (constantly current-time)]
      (testing "tracking-instance-killed"

        (scheduler/preserve-only-killed-instances-for-services! [])

        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-1" "A"))))
        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-2" "A"))))
        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-1" "C"))))
        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-1" "B"))))

        (is (= [(make-killed-instance "service-1" "A")
                (make-killed-instance "service-1" "B")
                (make-killed-instance "service-1" "C")]
               (scheduler/service-id->killed-instances "service-1")))
        (is (= [(make-killed-instance "service-2" "A")]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-1")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [(make-killed-instance "service-2" "A")]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-3" "A"))))
        (is (:killed? (scheduler/kill-instance dummy-scheduler (make-instance "service-3" "B"))))
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [(make-killed-instance "service-2" "A")]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [(make-killed-instance "service-3" "A")
                (make-killed-instance "service-3" "B")]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-2")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [(make-killed-instance "service-3" "A")
                (make-killed-instance "service-3" "B")]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))))))

(deftest test-delete-app
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s-name service-id :namespace "myself"})
        dummy-scheduler (make-dummy-scheduler [service-id])]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/delete-app dummy-scheduler service-id))]
          (is (= {:message (str "Kubernetes deleted ReplicaSet for " service-id)
                  :result :deleted}
                 actual))))
      (testing "unsuccessful-delete: service not found"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ url & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/delete-app dummy-scheduler service-id))]
          (is (= {:message "Kubernetes reports service does not exist"
                  :result :no-such-service-exists}
                 actual))))
      (testing "unsuccessful-delete: patch conflict"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ url & {:keys [request-method]}]
                                                 (if (= request-method :patch)
                                                   (ss/throw+ {:status 409})
                                                   {:spec {:replicas 1}}))]
                       (scheduler/delete-app dummy-scheduler service-id))]
          (is (= {:message "Kubernetes ReplicaSet conflict while deleting"
                  :result :conflict}
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw (RuntimeException.)))]
                       (scheduler/delete-app dummy-scheduler service-id))]
          (is (= {:message "Internal error while deleting service"
                  :result :error}
                 actual)))))))

(deftest test-kubernetes-scheduler
  (testing "Creating a KubernetesScheduler"

    (testing "should throw on invalid configuration"
      (testing "bad url"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url nil})))
      (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :max-conflict-retries 5
                                                    :max-name-length 63
                                                    :pod-base-port 8080
                                                    :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                    :url ""})))
      (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :max-conflict-retries 5
                                                    :max-name-length 63
                                                    :pod-base-port 8080
                                                    :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                    :url "localhost"}))))
      (testing "bad http options"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 0
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"})))
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 0}
                                                      :max-conflict-retries -1
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"}))))
      (testing "bad max conflict retries"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries -1
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"}))))
      (testing "bad max name length"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 0
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"}))))
      (testing "bad ReplicaSet spec path"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path nil
                                                      :url "http://127.0.0.1:8001"})))
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path ""
                                                      :url "http://127.0.0.1:8001"})))
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 8080
                                                      :rs-spec-file-path "/does/not/exist.edn"
                                                      :url "http://127.0.0.1:8001"}))))
      (testing "bad base port number"
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 80
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"})))
        (is (thrown? Throwable (kubernetes-scheduler {:authentication nil
                                                      :http-options {:conn-timeout 10000
                                                                     :socket-timeout 10000}
                                                      :max-conflict-retries 5
                                                      :max-name-length 63
                                                      :pod-base-port 50000
                                                      :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                                      :url "http://127.0.0.1:8001"})))))

    (testing "should work with valid configuration"
      (is (instance? KubernetesScheduler
                     (kubernetes-scheduler {:authentication nil
                                            :http-options {:conn-timeout 10000
                                                           :socket-timeout 10000}
                                            :max-conflict-retries 0
                                            :max-name-length 20
                                            :pod-base-port 1234
                                            :rs-spec-file-path "./specs/k8s-default-pod.edn"
                                            :url "http://127.0.0.1:8001"}))))))
