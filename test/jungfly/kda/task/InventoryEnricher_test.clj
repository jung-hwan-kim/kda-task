(ns jungfly.kda.task.InventoryEnricher-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [datascript.core :as d]
            [taoensso.nippy :as nippy]
            [jungfly.kda.task.mock.event :as data])
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

(defn parse-bstate[bstate-obj]
  (into {} (map (fn[x] {(.getKey x) (nippy/thaw (.getValue x))}) (seq (.entries bstate-obj)))))

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

(defn parse-kstate[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      nil
      (nippy/thaw v))))

(deftest enrich-test
  (let [f (new InventoryEnricher)
        c (new MockCollector)
        kso (new MockValueState)
        bso (new MockBroadcastState)
        vehicleId "12345678"
        data (data/parse (data/vehicle-update vehicleId))]
    (testing "Testing add, update remove flow"
      (let [result (.process f (nippy/freeze data) kso (.immutableEntries bso) c)
            collected (.getCollected c)
            bstate (parse-bstate bso)
            kstate (parse-kstate kso)]
        (log/info collected)
        (is (nil? result))
        (is (= 1 (count collected)))
        (is (= vehicleId (:vehicleId kstate)))
        (println kstate))
      (let [result (.process f (nippy/freeze data) kso (.immutableEntries bso) c)
            collected (.getCollected c)
            bstate (parse-bstate bso)
            kstate (parse-kstate kso)]
        (log/info collected)
        (is (nil? result))
        (is (= 2 (count collected)))
        (is (= vehicleId (:vehicleId kstate)))
        (println "** KSTATE **" kstate)
        kstate)
      )))

(deftest enrich-with-debug-test
  (let [f (new InventoryEnricher)
        c (new MockCollector)
        kso (new MockValueState)
        bso (new MockBroadcastState)
        vehicleId "12345678"
        data (assoc (data/parse (data/vehicle-update vehicleId)) :debug true)]
    (testing "Testing add, update remove flow"
      (let [result (.process f (json/encode-smile data) kso (.immutableEntries bso) c)
            collected (.getCollected c)
            bstate (parse-bstate bso)
            kstate (parse-kstate kso)]
        (log/info collected)
        (is (not (nil? result)))
        (is (= 1 (count collected)))
        (is (= vehicleId (:vehicleId kstate)))
        (println kstate))
      (let [result (.process f (json/encode-smile data) kso (.immutableEntries bso) c)
            collected (.getCollected c)
            bstate (parse-bstate bso)
            kstate (parse-kstate kso)]
        (log/info collected)
        (is ((not nil? result)))
        (is (= 2 (count collected)))
        (is (= vehicleId (:vehicleId kstate)))
        (println "** KSTATE **" kstate)
        kstate)
      )))