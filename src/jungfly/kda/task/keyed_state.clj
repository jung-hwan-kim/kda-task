(ns jungfly.kda.task.keyed-state
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(defn parse-kstate[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      {:history [] :picture [] :auction []}
      (json/decode-smile v true))))

(defn update-kstate-obj[kstate-obj kstate]
  (.update kstate-obj (json/encode-smile kstate)))

(defn vehicle-event[kstate event]
  (-> (merge kstate event)
      (update :history conj {:table (:eventtable event) :added (System/currentTimeMillis)})))

(defn picture-event[kstate event]
  (-> (merge kstate)
      (update :picture conj event)))

(defn auction-event[kstate event]
  (-> (merge kstate)
      (update :auction conj event)))

(defn operate-kstate[kstate-obj event]
  (let [eventtable (:eventtable event)
        kstate (parse-kstate kstate-obj)]
    (case eventtable
      ("ADLOAD.VEHICLES" "ADLOAD.VEHICLE_ADDITIONAL_INFOS") (let [new-kstate (vehicle-event kstate event)]
                                                              (update-kstate-obj kstate-obj new-kstate))
      "ADLOAD.PICTURES" (let [new-kstate (picture-event kstate event)]
                          (update-kstate-obj kstate-obj new-kstate))
      "ADLOAD.CURRENT_AUCTIONS" (let [new-kstate (auction-event kstate event)]
                          (update-kstate-obj kstate-obj new-kstate))
      (log/warn "Unsupported eventtable:" eventtable "->" event))))