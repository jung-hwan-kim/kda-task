(ns jungfly.kda.task.main-test
  (:require [clojure.test :refer :all]
            [jungfly.kda.task.mock.v :as v]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [jungfly.kda.task.main :refer :all]))



(def enricher (new jungfly.kda.task.Enricher))
(def op-enricher (new jungfly.kda.task.KeyedEnricher))
(def collector (new jungfly.kda.task.mock.MockCollector))

(defn convert-status [status-int]
  (case status-int
    1 "add"
    2 "update"
    3 "remove"
    "unknown"))
(defn transform [raw]
  (let [op (convert-status (:status raw))]
    (-> raw
        (assoc :created (System/currentTimeMillis))
        (update :id str)
        (assoc :type "actor")
        (assoc :op op)
        ;(update :status convert-status)
        (dissoc :status)
        (assoc :eventType op)
        )))
(defn push[flatMap e]
  (if (> (:status e) 0)
    (let [event (transform e)]
      (.flatMap flatMap (json/encode-smile event) collector))))


(defn change-and-push![flatMap]
  (let [v-data (v/change-and-update!)
        yin (:yin v-data)
        yang (:yang v-data)]
    (log/info v-data)
    (push flatMap yin)
    (push flatMap yang)))

(defn re-push[flatMap]
  (let [v-data @v/v
        yin (:yin v-data)
        yang (:yang v-data)]
    (log/info v-data)
    (push flatMap yin)
    (push flatMap yang)))
