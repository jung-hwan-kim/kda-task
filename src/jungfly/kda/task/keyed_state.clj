(ns jungfly.kda.task.keyed-state
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]))



(defn vehicle-event[kstate event]
  (-> (merge kstate event)
      (update :history (comp vec conj) {:table (:eventtable event) :added (System/currentTimeMillis)})))

(defn picture-event[kstate event]
  (-> (merge kstate)
      (update :pictures (comp vec conj) event)))

(defn auction-event[kstate event]
  (-> (merge kstate)
      (update :auctions (comp vec conj) event)))

(defn transform-kstate[kstate event]
  (let [eventtable (:eventtable event)]
    (case eventtable
      ("ADLOAD.VEHICLES" "ADLOAD.VEHICLE_ADDITIONAL_INFOS") (vehicle-event kstate event)
      "ADLOAD.PICTURES" (picture-event kstate event)
      "ADLOAD.CURRENT_AUCTIONS" (auction-event kstate event)
      (log/warn "Unsupported eventtable:" eventtable "->" event))))