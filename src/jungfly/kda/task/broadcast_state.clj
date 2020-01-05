(ns jungfly.kda.task.broadcast-state
  (:require [clojure.tools.logging :as log]
            [datascript.core :as d]
            [taoensso.nippy :as nippy]
            ))
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

(defn get-bstate-value[bstate-obj id]
  (nippy/thaw (.get bstate-obj id)))

(defn operate-bstate[bstate-obj event]
  (let [id (:id event)
        op (:op event)]
    (if-let [bstate-value (get-bstate-value bstate-obj id)]
      (case op
        "remove" (do
                   (.remove bstate-obj id))
        "update" (let [updated (updated-bstate-value bstate-value event)]
                   (.put bstate-obj id (nippy/freeze updated)))
        (log/error "INVALID B-STATE OPERATION:" + event))
      (do
        (.put bstate-obj id (nippy/freeze (new-bstate-value event)))))))

(defn inc-counter-smile [bstate-value-smile]
  (if (nil? bstate-value-smile)
    (let [aggr {:count 1 :UUID (.toString (java.util.UUID/randomUUID))}]
      (log/info aggr)
      (nippy/freeze aggr))
    (let [aggr (update (nippy/thaw bstate-value-smile) :count inc)]
      (log/info aggr)
      (nippy/freeze aggr))))

(defn update-bstate-counter [bstate-obj]
  (let [aggr (inc-counter-smile (.get bstate-obj "aggr"))]
    (.put bstate-obj "aggr" aggr)))
