(ns jungfly.kda.task.InventoryEnricher-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.data :as data])
  (:import (jungfly.kda.task.mock MockCollector MockBroadcastState MockValueState)
           (jungfly.kda.task InventoryEnricher)))


(defn do-process [function kstate bstate collector type op id]
  (.process function (json/encode-smile {:type type :op op :id id :created (System/currentTimeMillis)})
            kstate (.immutableEntries bstate) collector)
  (.getCollected collector))

(defn do-process-broadcast [function bstate collector type op id]
  (.processBroadcast function (json/encode-smile {:type type :op op :id id :created (System/currentTimeMillis)})
                     bstate collector)
  (.getCollected collector))




(defn do-process2 [function kstate bstate collector data]
  (.process function (json/encode-smile data)
            kstate (.immutableEntries bstate) collector)
  (.getCollected collector))


(defn parse-bstate[bstate]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq (.entries bstate)))))

(deftest b-test
  (let [f (new InventoryEnricher)
        c (new MockCollector)
        ks (new MockValueState)
        bs (new MockBroadcastState)]
    (testing "Testing add, update remove flow"
      (let [data (data/vehicle-update)
            collected (do-process2 f ks bs c data)
            bstate (parse-bstate bs)]
        (log/info collected)
        (is (= 1 (count collected)))
        (log/info bstate)
        ))))

(deftest rule-test
  (let [f (new InventoryEnricher)
        c (new MockCollector)
        ks (new MockValueState)
        s (new MockBroadcastState)]
    (testing "Testing rules: add, update remove flow"
      (let [collected (do-process-broadcast f s c "rule" "add" "1024")
            bstate (parse-bstate s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info bstate)
        (is (= 1 (get-in bstate ["aggr" :count]))))
      (let [ collected (do-process-broadcast f s c "rule" "update" "1024")
            bstate (parse-bstate s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info bstate)
        (is (= 2 (get-in bstate ["aggr" :count])))
        (is (= 2 (count bstate))))
      (let [ collected (do-process-broadcast f s c "rule" "update" "1024")
            bstate (parse-bstate s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info bstate)
        (is (= 3 (get-in bstate ["aggr" :count])))
        (is (= 2 (count bstate))))
      (let [ collected (do-process-broadcast f s c "rule" "remove" "1024")
            bstate (parse-bstate s)]
        (log/info collected)
        (is (= 0 (count collected)))
        (log/info bstate)
        (is (= 4 (get-in bstate ["aggr" :count])))
        (is (= 1 (count bstate))))
      )))

(defn parse-kstate[kstate]
  (let [v (.value kstate)]
    (if (nil? v)
      nil
      (json/decode-smile v true))))

(deftest enrich-test
  (let [f (new InventoryEnricher)
        c (new MockCollector)
        ks (new MockValueState)
        bs (new MockBroadcastState)
        data (data/vehicle-update)]
    (testing "Testing add, update remove flow"
      (let [ collected (do-process2 f ks bs c data)
            bstate (parse-bstate bs)
            kstate (parse-kstate ks)]
        (log/info collected)
        (is (= 1 (count collected)))
        (log/info "KSTATE" kstate)
        )
      (let [ collected (do-process2 f ks s c data)
            bstate (parse-bstate bs)
            kstate (parse-kstate ks)]
        (log/info collected)
        (is (= 2 (count collected)))
        (log/info "KSTATE" kstate)
        )
      )))