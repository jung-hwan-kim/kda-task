(ns jungfly.kda.task.Broadcaster-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (jungfly.kda.task.mock MockCollector MockBroadcastState)
           (jungfly.kda.task Broadcaster)))

(defn parse-key-value[node]
  (let [key (.getKey node) value (.getValue node)]
    {key value}))
(defn parse-stage[stage]
  (into {} (map parse-key-value (iterator-seq (.iterator (.entrySet stage))))))

(defn do-process [function state collector type op id]
  (.process function (json/encode-smile {:type type :op op :id id :created (System/currentTimeMillis)})
            (.immutableEntries state) collector)
  (.getCollected collector))

(defn do-process-broadcast [function state collector type op id]
  (.processBroadcast function (json/encode-smile {:type type :op op :id id :created (System/currentTimeMillis)})
            state collector)
  (.getCollected collector))


(defn parse-state[state]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq (.entries state)))))

(deftest b-test
  (let [f (new Broadcaster)
        c (new MockCollector)
        s (new MockBroadcastState)]
    (testing "Testing add, update remove flow"
      (let [ collected (do-process f s c "actor" "add" "1")
            state (parse-state s)]
        (log/info collected)
        (is (= 1 (count collected)))
        (log/info state)
        ))))

(deftest rule-test
  (let [f (new Broadcaster)
        c (new MockCollector)
        s (new MockBroadcastState)]
    (testing "Testing rules: add, update remove flow"
      (let [ collected (do-process-broadcast f s c "rule" "add" "1024")
            state (parse-state s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info state)
        (is (= 1 (get-in state ["aggr" :count])))
        )
      (let [ collected (do-process-broadcast f s c "rule" "update" "1024")
            state (parse-state s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info state)
        (is (= 2 (get-in state ["aggr" :count])))
        (is (= 2 (count state)))
        )
      (let [ collected (do-process-broadcast f s c "rule" "update" "1024")
            state (parse-state s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info state)
        (is (= 3 (get-in state ["aggr" :count])))
        (is (= 2 (count state)))
        )
      (let [ collected (do-process-broadcast f s c "rule" "remove" "1024")
            state (parse-state s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info state)
        (is (= 4 (get-in state ["aggr" :count])))
        (is (= 1 (count state)))
        )
        )))

(deftest enrich-test
  (let [f (new Broadcaster)
        c (new MockCollector)
        s (new MockBroadcastState)]
    (testing "Testing add, update remove flow"
      (let [ collected (do-process-broadcast f s c "rule" "add" "1024")
            state (parse-state s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info state)
        (is (= 1 (get-in state ["aggr" :count])))
        )
      (let [ collected (do-process f s c "actor" "add" "1")
            state (parse-state s)]
        (log/info collected)
        (is (= 1 (count collected)))
        (log/info state)
        )
      )))