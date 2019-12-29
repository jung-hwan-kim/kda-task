(ns jungfly.kda.task.OpEnricher
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractOpEnricher
    :exposes {staged {:get getStaged}}
    :main false
    ))

(defn describe-node[node]
  (let [id (.getKey node)
        actor (json/decode-smile (.getValue node) true)]
    {:ID id :ACT actor}))

(defn describe-stage[stage]
  (map describe-node (iterator-seq (.iterator (.entrySet stage)))))

(defn get-actor[stage id]
  ;(log/info "get-actor - id:" id)
  (let [b (.get stage id)]
    (json/decode-smile b true)))

(defn create-actor [event]
  ;(log/info "*** new actor:" event)
  (-> {:id (:id event)}
      (assoc :status "new")
      (assoc :created (:created event))
      (assoc :history [event])))

(defn update-counter [stage]
  (let [aggr-byte (.get stage "AGGR")]
    (if (nil? aggr-byte)
      (.put stage "AGGR" (json/encode-smile {:count 1}))
      (let [aggr (json/decode-smile aggr-byte true)]
        (.put stage "AGGR" (json/encode-smile (update aggr :count inc)))))))

(defn update-actor[actor event]
  ;(log/warn "update actor:" actor)
  ;(log/warn "update event" event)
  (-> actor
      (assoc :status "updated")
      (update :history conj event)))

;(defn remove-actor[stage id])

(defn -flatMap[this smile-data collector]
  (let [event (json/decode-smile smile-data true)
        stage (.getStaged this)]
    (log/info event)
    (update-counter stage)
    (if-let [id (:id event)]
      (if-let [actor (get-actor stage id)]
        (let [eventType (:eventType event)]
          (case eventType
            "remove" (.remove stage id)
            "update" (let [updated (json/encode-smile (update-actor actor event))]
                       ;(log/info event "->" (json/decode-smile updated true))
                       (.put stage id updated))
            (log/error "Invalid EVENT TYPE:" event)))
        (.put stage id (json/encode-smile (create-actor event))))
      (log/warn "!!! Stranger !!!"))
    (let [stage-info (describe-stage stage)]
      (log/info "STAGE" stage-info)
      (.collect collector (json/generate-string stage-info)))))
