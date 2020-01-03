(ns jungfly.kda.task.keyed-state
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(defn parse-kstate[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      {:history []}
      (json/decode-smile v true))))

(defn update-kstate-obj[kstate-obj kstate]
  (.update kstate-obj (json/encode-smile kstate)))

(defn vehicle-event[kstate event]
  (-> (merge kstate event)
      (update :history conj {:table (:eventtable event) :added (System/currentTimeMillis)})))


(defn operate-kstate[kstate-obj event]
  (let [eventtable (:eventtable event)
        kstate (parse-kstate kstate-obj)]
    (case eventtable
      ("ADLOAD.VEHICLES" "ADLOAD.VEHICLE_ADDITIONAL_INFOS") (let [new-kstate (vehicle-event kstate event)]
                                                              (update-kstate-obj kstate-obj new-kstate))
      (log/warn "Unsupported eventtable:" eventtable "->" event))
      ))