(ns jungfly.kda.task.keyed-state
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(defn parse-kstate[kstate]
  (let [v (.value kstate)]
    (if (nil? v)
      nil
      (json/decode-smile v true))))

(defn new-kstate-value[event]
  (-> {:vehicleId (:vehicleId event)}
      (assoc :status "new")
      (assoc :created (System/currentTimeMillis))
      (assoc :history [{:table (:EVENTTABLE event)}])))

(defn updated-kstate-value[ks event]
  (-> ks
      (assoc :status "updated")
      (assoc :updated (System/currentTimeMillis))
      (update :history conj {:table (:EVENTTABLE event)})))

(defn operate-kstate[kstate event]
  (let [op (:optype event)]
    (if-let [ks (parse-kstate kstate)]
      (case op
        "SQL COMPDELETE" (do
                           (.clear kstate))
        "SQL COMPUPDATE" (let [updated (updated-kstate-value ks event)]
                           (.update kstate (json/encode-smile updated)))
        (log/error "INVALID K-STATE OPERATION:" + event))
      (do
        (.update kstate (json/encode-smile (new-kstate-value event)))))))