(ns jungfly.kda.task.InventoryEnricher
  (:require [clojure.tools.logging :as log]
            [jungfly.kda.task.keyed-state :as ks]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractKeyedBroadcaster
    :exposes {sideTag {:get getSideTag}}
    :main false))

(defn new-bstate-value[event]
  (-> {:id (:id event)}
      (assoc :status "new")
      (assoc :created (System/currentTimeMillis))
      (assoc :history [event])))
(defn updated-bstate-value[value event]
  (-> value
      (assoc :status "updated")
      (assoc :updated (System/currentTimeMillis))
      (update :history conj event)))

(defn get-bstate-value[state id]
  (json/decode-smile (.get state id)))

(defn operate-bstate[bstate event]
  (let [id (:id event)
        op (:op event)]
    (if-let [bstate-value (get-bstate-value bstate id)]
      (case op
        "remove" (do
                   (.remove bstate id))
        "update" (let [updated (updated-bstate-value bstate-value event)]
                   (.put bstate id (json/encode-smile updated)))
        (log/error "INVALID B-STATE OPERATION:" + event))
      (do
        (.put bstate id (json/encode-smile (new-bstate-value event)))))))

(defn inc-counter-smile [smile]
  (if (nil? smile)
    (let [aggr {:count 1 :UUID (.toString (java.util.UUID/randomUUID))}]
      (log/info aggr)
      (json/encode-smile aggr))
    (let [aggr (update (json/decode-smile smile true) :count inc)]
      (log/info aggr)
      (json/encode-smile aggr))))

(defn update-bstate-counter [bstate]
  (let [aggr (inc-counter-smile (.get bstate "aggr"))]
    (.put bstate "aggr" aggr)))


(defn describe-bstate[bstate]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq (.entries bstate)))))

(defn describe-bstate-iterable[bstate-iterable]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq bstate-iterable)))
  )


(defn parse-kstate[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      nil
      (json/decode-smile v true))))

(defn update-kstate-obj[kstate-obj kstate]
  (.update kstate-obj (json/encode-smile kstate)))


(defn -process[this smile-data kstate-obj bstate-iterable collector]
  (let [event (json/decode-smile smile-data true)
        kstate (parse-kstate kstate-obj)
        bstate (describe-bstate-iterable bstate-iterable)
        eventtable (:eventtable event)]
    (log/info "process:" event)
    (if (= eventtable "HEARTBEAT_K")
      (case (:action event)
        "cleanup" (do
                    (.clear kstate-obj)
                    (.collect collector (json/encode-smile (assoc event :cleaned-up kstate))))
        "kstate" (.collect collector (json/encode-smile (assoc event :kstate kstate)))
        "bstate" (.collect collector (json/encode-smile (assoc event :bstate bstate)))
        (.collect collector smile-data))
      (if-let [new-kstate (ks/transform-kstate kstate event)]
        (do
          (update-kstate-obj kstate-obj new-kstate)
          (.collect collector (json/encode-smile new-kstate)))
        (log/info "NO UPDATE")))
    (if (= (:debug event) true)
        (json/generate-string {:kstate (parse-kstate kstate-obj) :bstate bstate}))))

(defn -processBroadcast[this smile-data bstate collector]
  (let [event (json/decode-smile smile-data true)]
    (log/info "process-broadcast:" event)
    (update-bstate-counter bstate)
    (operate-bstate bstate event)
    (json/generate-string (describe-bstate bstate))))
