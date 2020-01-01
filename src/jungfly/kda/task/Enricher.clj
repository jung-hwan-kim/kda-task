(ns jungfly.kda.task.Enricher
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractEnricher
    :exposes {staged {:get getStaged}}
    :main false
    )
  )

(defn describe-node[node]
  (let [id (.getKey node)
        actor (.getValue node)]
    {:ID id :ACT actor}))

(defn describe-stage[stage]
  (map describe-node (iterator-seq (.iterator (.entrySet stage)))))

(defn get-actor[stage id]
  ;(log/info "get-actor - id:" id)
  (.get stage id))

(defn create-actor [event]
  ;(log/info "*** new actor:" event)
  (-> {:id (:id event)}
      (assoc :status "new")
      (assoc :created (:created event))
      (assoc :history [event])))

(defn update-counter [stage]
  (let [aggr (.get stage "AGGR")]
    (if (nil? aggr)
      (.put stage "AGGR" {:count 1 :UUID (.toString (java.util.UUID/randomUUID))})
      (.put stage "AGGR" (update aggr :count inc)))))

(defn update-actor[actor event]
  ;(log/warn "update actor:" actor)
  ;(log/warn "update event" event)
  (-> actor
      (assoc :status "updated")
      (update :history conj event)))

;(defn remove-actor[stage id])

(defn -serialize[this m]
  (json/encode-smile m))

(defn -deserialize[this b]
  (json/decode-smile b true))

(defn operate[stage event]
  (let [id (:id event)
        op (:op event)]
    (if-let [actor (get-actor stage id)]
      (case op
        "remove" (do
                  (.remove stage id)
                  (assoc event :action "removed"))
        "update" (let [updated (update-actor actor event)]
                   (.put stage id updated)
                   (assoc event :action "updated"))
        (assoc event :error (str "Invalid EVENT TYPE:" op)))
      (do
        (.put stage id (create-actor event))
        (assoc event :action "added")))))


(defn -flatMap[this smile-data collector]
  (let [
        event (json/decode-smile smile-data true)
        stage (.getStaged this)]
    (log/info event)
    (update-counter stage)
    (let [r (assoc (operate stage event) :processed (System/currentTimeMillis))]
      (.collect collector (json/encode-smile r)))
    (let [stage-info (describe-stage stage)]
      (log/info "STAGE" stage-info))))
