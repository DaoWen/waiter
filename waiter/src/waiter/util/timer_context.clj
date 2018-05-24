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
(ns waiter.util.timer-context
  (:require [clj-time.core :as t])
  (:import (com.codahale.metrics Timer)
           (java.util.concurrent TimeUnit)))

(defrecord TimerContext [timer start-time])

(defn report-duration
  "Report elapsed duration on the given timer metric. Returns nil."
  [{:keys [timer start-time]} end-time]
  (.update ^Timer timer
           (t/in-millis (t/interval start-time end-time))
           TimeUnit/MILLISECONDS))
