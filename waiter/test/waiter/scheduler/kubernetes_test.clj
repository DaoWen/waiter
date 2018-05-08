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
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.scheduler.kubernetes :refer :all]
            [waiter.scheduler :as scheduler]
            [waiter.util.date-utils :as du])
  (:import waiter.scheduler.kubernetes.KubernetesScheduler))

(deftest test-respone-data->service-instances
  (let [test-cases [{:name "response-data->service-instances no response"
                     :kubernetes-response nil
                     :expected-response {:active-instances []
                                         :failed-instances []}}

                    {:name "response-data->service-instances empty response"
                     :kubernetes-response {}
                     :expected-response {:active-instances []
                                         :failed-instances []}}

                    {:name "response-data->service-instances empty-app response"
                     :kubernetes-response {:apiVersion "v1" :items [] :kind "List"
                                           :metadata {:resourceVersion "" :selfLink ""}}
                     :expected-response {:active-instances []
                                         :failed-instances []}}

                    {:name "response-data->service-instances valid response with task failure"
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
                                            :started-at (du/str-to-date "2014-09-12T23:23:41Z" k8s-timestamp-format)})]}}

                    {:name "response-data->service-instances valid response without task failure"
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
                      :failed-instances []}}]]
    (doseq [{:keys [expected-response kubernetes-response name]} test-cases]
      (testing (str "Test " name)
        (let [dummy-service {:id "test-app-1234"}
              dummy-scheduler {:service-id->failed-instances-transient-store (atom {})}
              actual-response (with-redefs [;; mock the K8s API server returning our test responses
                                            api-request (constantly kubernetes-response)]
                                (->> (instances-breakdown dummy-service dummy-scheduler)
                                     ;; strip out extra fields added to K8s-specific records
                                     (pc/map-vals (partial mapv #(dissoc % :k8s-name :namespace :pod-name :restart-count)))))]
          (is (= expected-response actual-response) (str name))
          (scheduler/preserve-only-killed-instances-for-services! []))))))

(comment "Disabled tests"

(deftest test-response-data->service->service-instances
  (let [input [{:id "test-app-1234"
                :instances 2
                :healthChecks [{:path "/ping", :portIndex 0, :protocol "HTTPS", :timeoutSeconds 10}]
                :tasks [{:appId "/test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-1234.A"}]
                         :host "10.141.141.11"
                         :id "test-app-1234.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T23:28:28.594Z"
                         :startedAt "2014-09-13T00:24:46.959Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.508Z"
                                               :taskId "test-app-1234.B"}]
                         :host "10.141.141.12"
                         :id "test-app-1234.B"
                         :ports [31234]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-13T00:24:46.965Z"
                         :version "2014-09-12T23:28:21.737Z"}]
                :tasksRunning 2
                :tasksHealthy 2
                :tasksUnhealthy 0
                :tasksStaged 0}
               {:id "test-app-6789"
                :instances 3
                :lastTaskFailure {:appId "test-app-6789"
                                  :host "10.141.141.10"
                                  :message "Abnormal executor termination"
                                  :state "TASK_FAILED"
                                  :taskId "test-app-6789.D"
                                  :timestamp "2014-09-12T23:23:41.711Z"
                                  :version "2014-09-12T23:28:21.737Z"}
                :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTP", :timeoutSeconds 10}]
                :tasks [{:appId "test-app-6789"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-6789.A"}]
                         :host "10.141.141.11"
                         :id "/test-app-6789.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T23:28:28.594Z"
                         :startedAt "2014-09-13T00:24:46.959Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "/test-app-6789"
                         :host "10.141.141.12"
                         :id "/test-app-6789.B"
                         :ports [36789]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-13T00:24:56.965Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "/test-app-6789"
                         :healthCheckResults [{:alive false
                                               :consecutiveFailures 10
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure "2014-09-13T002507.508Z"
                                               :lastSuccess nil
                                               :taskId "test-app-6789.C"}]
                         :host "10.141.141.13"
                         :id "test-app-6789.C"
                         :ports [46789]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-14T00:24:46.965Z"
                         :version "2014-09-12T23:28:21.737Z"}]}]
        expected (assoc {}
                        (scheduler/make-Service {:id "test-app-1234"
                                                 :instances 2
                                                 :task-count 2
                                                 :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                        {:active-instances
                         (list
                           (scheduler/make-ServiceInstance
                             {:id "test-app-1234.A"
                              :service-id "test-app-1234"
                              :healthy? true
                              :host "10.141.141.11"
                              :port 31045
                              :protocol "https"
                              :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" k8s-timestamp-format)})
                           (scheduler/make-ServiceInstance
                             {:id "test-app-1234.B"
                              :service-id "test-app-1234"
                              :healthy? true
                              :host "10.141.141.12"
                              :port 31234
                              :protocol "https"
                              :started-at (du/str-to-date "2014-09-13T00:24:46.965Z" k8s-timestamp-format)}))
                         :failed-instances []
                         :killed-instances []}
                        (scheduler/make-Service {:id "test-app-6789", :instances 3, :task-count 3})
                        {:active-instances
                         (list
                           (scheduler/make-ServiceInstance
                             {:id "test-app-6789.A"
                              :service-id "test-app-6789"
                              :healthy? true
                              :host "10.141.141.11"
                              :port 31045
                              :protocol "http"
                              :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" k8s-timestamp-format)})
                           (scheduler/make-ServiceInstance
                             {:id "test-app-6789.B"
                              :service-id "test-app-6789"
                              :healthy? nil
                              :host "10.141.141.12"
                              :port 36789
                              :protocol "http"
                              :started-at (du/str-to-date "2014-09-13T00:24:56.965Z" k8s-timestamp-format)})
                           (scheduler/make-ServiceInstance
                             {:id "test-app-6789.C"
                              :service-id "test-app-6789"
                              :healthy? false
                              :host "10.141.141.13"
                              :port 46789
                              :protocol "http"
                              :started-at (du/str-to-date "2014-09-14T00:24:46.965Z" k8s-timestamp-format)}))
                         :failed-instances
                         (list
                           (scheduler/make-ServiceInstance
                             {:id "test-app-6789.D"
                              :service-id "test-app-6789"
                              :healthy? false
                              :host "10.141.141.10"
                              :port 0
                              :protocol "http"
                              :started-at (du/str-to-date "2014-09-12T23:23:41.711Z" k8s-timestamp-format)
                              :message "Abnormal executor termination"}))
                         :killed-instances []})
        service-id->failed-instances-transient-store (atom {})
        service-id->service-description {"test-app-1234" {"backend-proto" "https"}
                                         "test-app-6789" {"backend-proto" "http"}}
        actual (response-data->service->service-instances
                 input (fn [] nil) nil service-id->failed-instances-transient-store service-id->service-description)]
    (is (= expected actual))
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])))

(deftest test-service-id->failed-instances-transient-store
  (let [faled-instance-response-fn (fn [service-id instance-id]
                                     {:appId service-id,
                                      :host (str "10.141.141." instance-id),
                                      :message "Abnormal executor termination",
                                      :state (str instance-id "failed"),
                                      :taskId (str service-id "." instance-id),
                                      :timestamp "2014-09-12T23:23:41.711Z",
                                      :version "2014-09-12T23:28:21.737Z"})
        framework-id "framework-id"
        health-check-url "/status"
        slave-directory "/slave"
        common-extractor-fn (fn [instance-id kubernetes-task-response]
                              (let [{:keys [appId host message slaveId]} kubernetes-task-response]
                                (cond-> {:service-id appId
                                         :host host
                                         :health-check-path health-check-url}
                                  (and framework-id slaveId)
                                  (assoc :log-directory
                                         (str slave-directory "/" slaveId "/frameworks/" framework-id
                                              "/executors/" instance-id "/runs/latest"))
                                  message
                                  (assoc :message (str/trim message)))))
        service-id-1 "test-service-id-failed-instances-1"
        service-id-2 "test-service-id-failed-instances-2"
        service-id->failed-instances-transient-store (atom {})]
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "C") common-extractor-fn)
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "D") common-extractor-fn)
    (is (= 4 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "C") common-extractor-fn)
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "D") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "E") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "F") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "G") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "H") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "I") common-extractor-fn)
    (is (= 9 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "X") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "Y") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "Z") common-extractor-fn)
    (remove-failed-instances-for-service! service-id->failed-instances-transient-store service-id-1)
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [service-id-2])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))))

(deftest test-retrieve-log-url
  (let [instance-id "service-id-1.instance-id-2"
        host "www.example.com"
        mesos-api (Object.)]
    (with-redefs [mesos/get-agent-state
                  (fn [in-mesos-api in-host]
                    (is (= mesos-api in-mesos-api))
                    (is (= host in-host))
                    (let [response-body "
                          {
                                        \"frameworks\": [{
                                        \"role\": \"kubernetes\",
                                        \"completed_executors\": [{
                                        \"id\": \"service-id-1.instance-id-1\",
                                        \"directory\": \"/path/to/instance1/directory\"
                                        }],
                                        \"executors\": [{
                                        \"id\": \"service-id-1.instance-id-2\",
                                        \"directory\": \"/path/to/instance2/directory\"
                                        }]
                                        }]
                                        }"]
                      (-> response-body json/read-str walk/keywordize-keys)))]
      (is (= "/path/to/instance2/directory" (retrieve-log-url mesos-api instance-id host))))))

(deftest test-retrieve-directory-content-from-host
  (let [service-id "service-id-1"
        instance-id "service-id-1.instance-id-2"
        host "www.example.com"
        mesos-slave-port 5051
        directory "/path/to/instance2/directory"
        mesos-api (mesos/api-factory (Object.) {} mesos-slave-port directory)]
    (with-redefs [mesos/list-directory-content
                  (fn [in-mesos-api in-host in-directory]
                    (is (= mesos-api in-mesos-api))
                    (is (= host in-host))
                    (is (= directory in-directory))
                    (let [response-body "
                          [{\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil1\", \"size\": 1000},
                                        {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir2\", \"size\": 2000},
                                        {\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil3\", \"size\": 3000},
                                        {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir4\", \"size\": 4000}]"]
                      (-> response-body json/read-str walk/keywordize-keys)))]
      (let [expected-result (list {:name "fil1"
                                   :size 1000
                                   :type "file"
                                   :url "http://www.example.com:5051/files/download?path=/path/to/instance2/directory/fil1"}
                                  {:name "dir2"
                                   :size 2000
                                   :type "directory"
                                   :path "/path/to/instance2/directory/dir2"}
                                  {:name "fil3"
                                   :size 3000
                                   :type "file"
                                   :url "http://www.example.com:5051/files/download?path=/path/to/instance2/directory/fil3"}
                                  {:name "dir4"
                                   :size 4000
                                   :type "directory"
                                   :path "/path/to/instance2/directory/dir4"})]
        (is (= expected-result (retrieve-directory-content-from-host mesos-api service-id instance-id host directory)))))))

(deftest test-kubernetes-descriptor
  (let [service-id->password-fn (fn [service-id] (str service-id "-password"))]
    (testing "basic-test-with-defaults"
      (let [expected {:id "test-service-1"
                      :labels {:source "waiter"
                               :user "test-user"}
                      :env {"BAZ" "quux"
                            "FOO" "bar"
                            "HOME" "/home/path/test-user"
                            "LOGNAME" "test-user"
                            "USER" "test-user"
                            "WAITER_CPUS" "1"
                            "WAITER_MEM_MB" "1536"
                            "WAITER_PASSWORD" "test-service-1-password"
                            "WAITER_SERVICE_ID" "test-service-1"
                            "WAITER_USERNAME" "waiter"}
                      :cmd "test-command"
                      :cpus 1
                      :disk nil
                      :mem 1536
                      :healthChecks [{:protocol "HTTP"
                                      :path "/status"
                                      :gracePeriodSeconds 111
                                      :intervalSeconds 10
                                      :portIndex 0
                                      :timeoutSeconds 20
                                      :maxConsecutiveFailures 5}]
                      :backoffFactor 2
                      :ports [0 0]
                      :user "test-user"}
            home-path-prefix "/home/path/"
            service-id "test-service-1"
            service-description {"backend-proto" "http"
                                 "cmd" "test-command"
                                 "cpus" 1
                                 "mem" 1536
                                 "run-as-user" "test-user"
                                 "ports" 2
                                 "restart-backoff-factor" 2
                                 "grace-period-secs" 111
                                 "health-check-interval-secs" 10
                                 "health-check-max-consecutive-failures" 5
                                 "env" {"FOO" "bar"
                                        "BAZ" "quux"}}
            actual (kubernetes-descriptor home-path-prefix service-id->password-fn
                                          {:service-id service-id, :service-description service-description})]
        (is (= expected actual))))))

(deftest test-kill-instance-last-force-kill-time-store
  (let [current-time (t/now)
        service-id "service-1"
        instance-id "service-1.A"
        make-kubernetes-scheduler #(->MarathonScheduler {} {} (constantly nil) "/home/path/"
                                                        (atom {}) %1 (constantly nil) %2 (constantly true))
        successful-kill-result {:instance-id instance-id :killed? true :service-id service-id}
        failed-kill-result {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [t/now (fn [] current-time)]

      (testing "normal-kill"
        (let [service-id->kill-info-store (atom {})
              kubernetes-scheduler (make-kubernetes-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance kubernetes-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store)))))

      (testing "failed-kill"
        (let [service-id->kill-info-store (atom {})
              kubernetes-scheduler (make-kubernetes-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance kubernetes-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since current-time}} @service-id->kill-info-store)))))

      (testing "not-yet-forced"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}})
              kubernetes-scheduler (make-kubernetes-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance kubernetes-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}} @service-id->kill-info-store)))))

      (testing "forced-kill"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 1500))}})
              kubernetes-scheduler (make-kubernetes-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force true, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance kubernetes-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store))))))))

(deftest test-service-id->state
  (let [service-id "service-id"
        kubernetes-scheduler (->MarathonScheduler {} {} (constantly nil) "/home/path/"
                                                  (atom {service-id [:failed-instances]})
                                                  (atom {service-id :kill-call-info})
                                                  (constantly nil) 100 (constantly true))
        state (scheduler/service-id->state kubernetes-scheduler service-id)]
    (is (= {:failed-instances [:failed-instances], :killed-instances [], :kill-info :kill-call-info} state))))

(deftest test-killed-instances-transient-store
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        kubernetes-api (Object.)
        kubernetes-scheduler (->MarathonScheduler kubernetes-api {} (constantly nil) "/home/path/"
                                                  (atom {}) (atom {}) (constantly nil) 60000 (constantly true))
        make-instance (fn [service-id instance-id]
                        {:id instance-id
                         :service-id service-id})]
    (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api service-id instance-id scale-value force-value]
                                         (is (= kubernetes-api in-kubernetes-api))
                                         (is (= [scale-value force-value] [true false]))
                                         {:service-id service-id, :instance-id instance-id, :killed? true, :deploymentId "12982340972"})
                  t/now (fn [] current-time)]
      (testing "tracking-instance-killed"

        (scheduler/preserve-only-killed-instances-for-services! [])

        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-1" "service-1.A"))))
        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-2" "service-2.A"))))
        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-1" "service-1.C"))))
        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-1" "service-1.B"))))

        (is (= [{:id "service-1.A", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.B", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.C", :service-id "service-1", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-1")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-3" "service-3.A"))))
        (is (:killed? (scheduler/kill-instance kubernetes-scheduler (make-instance "service-3" "service-3.B"))))
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-2")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))))))

(deftest test-max-failed-instances-cache
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time k8s-timestamp-format)
        service-id->failed-instances-transient-store (atom {})
        common-extractor-fn (constantly {:service-id "service-1"})]
    (testing "test-max-failed-instances-cache"
      (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
      (doseq [n (range 10 50)]
        (parse-and-store-failed-instance!
          service-id->failed-instances-transient-store
          "service-1"
          {:taskId (str "service-1." n)
           :timestamp current-time-str}
          common-extractor-fn))
      (let [actual-failed-instances (set (service-id->failed-instances service-id->failed-instances-transient-store "service-1"))]
        (is (= 10 (count actual-failed-instances)))
        (doseq [n (range 40 50)]
          (is (contains? actual-failed-instances
                         (scheduler/make-ServiceInstance
                           {:id (str "service-1." n)
                            :service-id "service-1"
                            :started-at (du/str-to-date current-time-str k8s-timestamp-format)
                            :healthy? false
                            :port 0}))
              (str "Failed instances does not contain instance service-1." n)))))))

(deftest test-kubernetes-scheduler
  (testing "Creating a MarathonScheduler"

    (testing "should throw on invalid configuration"
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 900000
                                                    :home-path-prefix "/home/"
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :mesos-slave-port 5051
                                                    :slave-directory "/foo"
                                                    :url nil})))
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 900000
                                                    :home-path-prefix "/home/"
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :mesos-slave-port 5051
                                                    :slave-directory ""
                                                    :url "url"})))
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 900000
                                                    :home-path-prefix "/home/"
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :mesos-slave-port 0
                                                    :slave-directory "/foo"
                                                    :url "url"})))
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 900000
                                                    :home-path-prefix "/home/"
                                                    :http-options {}
                                                    :mesos-slave-port 5051
                                                    :slave-directory "/foo"
                                                    :url "url"})))
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 900000
                                                    :home-path-prefix nil
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :mesos-slave-port 5051
                                                    :slave-directory "/foo"
                                                    :url "url"})))
      (is (thrown? Throwable (kubernetes-scheduler {:framework-id-ttl 0
                                                    :home-path-prefix "/home/"
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :mesos-slave-port 5051
                                                    :slave-directory "/foo"
                                                    :url "url"}))))

    (testing "should work with valid configuration"
      (is (instance? MarathonScheduler
                     (kubernetes-scheduler {:home-path-prefix "/home/"
                                            :http-options {:conn-timeout 10000
                                                           :socket-timeout 10000}
                                            :force-kill-after-ms 60000
                                            :framework-id-ttl 900000
                                            :url "url"}))))))

(deftest test-process-kill-instance-request
  (let [kubernetes-api (Object.)
        service-id "test-service-id"
        instance-id "instance-id"]
    (testing "successful-delete"
      (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api in-service-id in-instance-id scale-value force-value]
                                           (is (= kubernetes-api in-kubernetes-api))
                                           (is (= service-id in-service-id))
                                           (is (= instance-id in-instance-id))
                                           (is (= [scale-value force-value] [true false]))
                                           {:deploymentId "12982340972"})]
        (is (= {:instance-id instance-id :killed? true :message "Successfully killed instance" :service-id service-id, :status 200}
               (process-kill-instance-request kubernetes-api service-id instance-id {})))))

    (testing "unsuccessful-delete"
      (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api in-service-id in-instance-id scale-value force-value]
                                           (is (= kubernetes-api in-kubernetes-api))
                                           (is (= service-id in-service-id))
                                           (is (= instance-id in-instance-id))
                                           (is (= [scale-value force-value] [true false]))
                                           {:failed true})]
        (is (= {:instance-id instance-id :killed? false :message "Unable to kill instance" :service-id service-id, :status 500}
               (process-kill-instance-request kubernetes-api service-id instance-id {})))))

    (testing "deployment-conflict"
      (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api in-service-id in-instance-id scale-value force-value]
                                           (is (= kubernetes-api in-kubernetes-api))
                                           (is (= service-id in-service-id))
                                           (is (= instance-id in-instance-id))
                                           (is (= [scale-value force-value] [true false]))
                                           (ss/throw+ {:status 409}))]
        (is (= {:instance-id instance-id :killed? false :message "Locked by one or more deployments" :service-id service-id, :status 409}
               (process-kill-instance-request kubernetes-api service-id instance-id {})))))

    (testing "kubernetes-404"
      (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api in-service-id in-instance-id scale-value force-value]
                                           (is (= kubernetes-api in-kubernetes-api))
                                           (is (= service-id in-service-id))
                                           (is (= instance-id in-instance-id))
                                           (is (= [scale-value force-value] [true false]))
                                           (ss/throw+ {:body "Not Found", :status 404}))]
        (is (= {:instance-id instance-id :killed? false :message "Not Found" :service-id service-id, :status 404}
               (process-kill-instance-request kubernetes-api service-id instance-id {})))))

    (testing "exception-while-killing"
      (with-redefs [kubernetes/kill-task (fn [in-kubernetes-api in-service-id in-instance-id scale-value force-value]
                                           (is (= kubernetes-api in-kubernetes-api))
                                           (is (= service-id in-service-id))
                                           (is (= instance-id in-instance-id))
                                           (is (= [scale-value force-value] [true false]))
                                           (throw (Exception. "exception from test")))]
        (is (= {:instance-id instance-id :killed? false :message "exception from test" :service-id service-id, :status 500}
               (process-kill-instance-request kubernetes-api service-id instance-id {})))))))

(deftest test-delete-app
  (let [scheduler (->MarathonScheduler {} {} nil nil (atom {}) (atom {}) (constantly nil) nil nil)]

    (with-redefs [kubernetes/delete-app (constantly {:deploymentId 12345})]
      (is (= {:result :deleted
              :message "Marathon deleted with deploymentId 12345"}
             (scheduler/delete-app scheduler "foo"))))

    (with-redefs [kubernetes/delete-app (constantly {})]
      (is (= {:result :error
              :message "Marathon did not provide deploymentId for delete request"}
             (scheduler/delete-app scheduler "foo"))))

    (with-redefs [kubernetes/delete-app (fn [_ _] (ss/throw+ {:status 404}))]
      (is (= {:result :no-such-service-exists
              :message "Marathon reports service does not exist"}
             (scheduler/delete-app scheduler "foo"))))))

(deftest test-extract-deployment-info
  (let [kubernetes-api (Object.)
        prepare-response (fn [body]
                           (let [body-chan (async/promise-chan)]
                             (async/>!! body-chan body)
                             {:body body-chan}))]
    (with-redefs [kubernetes/get-deployments (constantly [{"affectedApps" "waiter-app-1234", "id" "1234", "version" "v1234"}
                                                          {"affectedApps" "waiter-app-4567", "id" "4567", "version" "v4567"}
                                                          {"affectedApps" "waiter-app-3829", "id" "3829", "version" "v3829"}
                                                          {"affectedApps" "waiter-app-4321", "id" "4321", "version" "v4321"}])]
      (testing "no deployments entry"
        (let [response (prepare-response "{\"message\": \"App is locked by one or more deployments.\"}")]
          (is (not (extract-deployment-info kubernetes-api response)))))

      (testing "no deployments listed"
        (let [response (prepare-response "{\"deployments\": [],
                                         \"message\": \"App is locked by one or more deployments.\"}")]
          (is (not (extract-deployment-info kubernetes-api response)))))

      (testing "single deployment"
        (let [response (prepare-response "{\"deployments\": [{\"id\":\"1234\"}],
                                         \"message\": \"App is locked by one or more deployments.\"}")]
          (is (= [{"affectedApps" "waiter-app-1234", "id" "1234", "version" "v1234"}]
                 (extract-deployment-info kubernetes-api response)))))

      (testing "multiple deployments"
        (let [response {:body "{\"deployments\": [{\"id\":\"1234\"}, {\"id\":\"3829\"}],
                              \"message\": \"App is locked by one or more deployments.\"}"}]
          (is (= [{"affectedApps" "waiter-app-1234", "id" "1234", "version" "v1234"}
                  {"affectedApps" "waiter-app-3829", "id" "3829", "version" "v3829"}]
                 (extract-deployment-info kubernetes-api response)))))

      (testing "multiple deployments, one without info"
        (let [response (prepare-response "{\"deployments\": [{\"id\":\"1234\"}, {\"id\":\"3829\"}, {\"id\":\"9876\"}],
                                         \"message\": \"App is locked by one or more deployments.\"}")]
          (is (= [{"affectedApps" "waiter-app-1234", "id" "1234", "version" "v1234"}
                  {"affectedApps" "waiter-app-3829", "id" "3829", "version" "v3829"}]
                 (extract-deployment-info kubernetes-api response))))))))
)
