(ns jungfly.kda.task.InventoryEnricher
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractKeyedBroadcaster
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


(defn parse-kstate[kstate]
  (let [v (.value kstate)]
    (if (nil? v)
      nil
      (json/decode-smile v true))))

(defn new-kstate-value[event]
  (-> {:VEHICLE_ID (:VEHICLE_ID event)}
      (assoc :status "new")
      (assoc :created (System/currentTimeMillis))
      (assoc :history [{:table (:EVENTTABLE event)}])))

(defn updated-kstate-value[ks event]
  (-> ks
      (assoc :status "updated")
      (assoc :updated (System/currentTimeMillis))
      (update :history conj {:table (:EVENTTABLE event)})))

(defn operate-kstate[kstate event]
  (let [op (:OPTYPE event)]
    (if-let [ks (parse-kstate kstate)]
      (case op
        "SQL COMPDELETE" (do
                   (.clear kstate))
        "SQL COMPUPDATE" (let [updated (updated-kstate-value ks event)]
                   (.update kstate (json/encode-smile updated)))
        (log/error "INVALID K-STATE OPERATION:" + event))
      (do
        (.update kstate (json/encode-smile (new-kstate-value event)))))))


(defn enrich[event kstate bstate-iterable]
  (-> event
      (assoc :bstate (describe-bstate-iterable bstate-iterable))
      (assoc :kstate (parse-kstate kstate))
      ))

(defn -process[this smile-data kstate bstate-iterable collector]
  (let [event (json/decode-smile smile-data true)
        ks (parse-kstate kstate)]
    (log/info "process:" event)
    (operate-kstate kstate event)
    (.collect collector (json/encode-smile (enrich event kstate bstate-iterable)))))

(defn -processBroadcast[this smile-data bstate collector]
  (let [event (json/decode-smile smile-data true)]
    (log/info "process-broadcast:" event)
    (update-bstate-counter bstate)
    (operate-bstate bstate event)
    ))
