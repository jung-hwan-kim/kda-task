(ns jungfly.kda.task.Enricher-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (jungfly.kda.task.mock MockCollector)
           (jungfly.kda.task Enricher)))


(deftest a-test
  (let [f (new Enricher)
        collector (new MockCollector)
        rawEvent (json/encode-smile {:id "1" :type "TestType"})]
    (testing "running map function"
      (.flatMap f rawEvent collector)
      (let [collected (.getCollected collector)]
        (log/info collected)
        (is (= 1 (count collected)))
        )
      (.flatMap f rawEvent collector)
      (let [collected (.getCollected collector)]
        (log/info collected)
        (is (= "Invalid EVENT TYPE:" (:error (second collected))))
        (is (= 2 (count collected)))))))

(defn parse-key-value[node]
  (let [key (.getKey node) value (.getValue node)]
    {key value}))
(defn parse-stage[stage]
  (into {} (map parse-key-value (iterator-seq (.iterator (.entrySet stage))))))

(defn do-flat-map [function type op id collector]
  (.flatMap function (json/encode-smile {:type type :op op :id id :created (System/currentTimeMillis)}) collector)
  (list (parse-stage (.getStaged function)) (.getCollected collector)))

(deftest b-test
  (let [f (new Enricher)
        c (new MockCollector)]
    (testing "Testing add, update remove flow"
      (let [[stage collected] (do-flat-map f "actor" "add" "1" c)]
        (log/info collected)
        (is (= 1 (count collected)))
        (log/info stage)
        (is (= 1 (get-in stage ["AGGR" :count])))
        (is (= 1 (count (get-in stage ["1" :history]))))
        )
      (let [[stage collected] (do-flat-map f "actor" "update" "1" c)]
        (log/info collected)
        (is (= "updated" (:action (last collected))))
        (is (= 2 (count collected)))
        (log/info stage)
        (is (= 2 (get-in stage ["AGGR" :count])))
        (is (= 2 (count (get-in stage ["1" :history]))))
        )
      (let [[stage collected] (do-flat-map f "actor" "remove" "1" c)]
        (log/info collected)
        (is (= "removed" (:action (last collected ))))
        (is (= 3 (count collected)))
        (log/info stage)
        (is (= 3 (get-in stage ["AGGR" :count])))
        (is (nil? (get stage "1")))  ; make sure it's cleaned up
        ))))



