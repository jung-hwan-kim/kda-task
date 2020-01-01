(ns jungfly.kda.task.Broadcaster
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractBroadcaster
    :main false
    )
  (:import (jungfly.kda.task RawEvent)))

(defn describe-node[node]
  (let [key (.getKey node)
        value (.getValue node)]
    {:key key :value value}))

(defn describe-rulebook[rulebook]
  (map describe-node (iterator-seq (.iterator (.entrySet rulebook)))))

(defn get-rule[stage id]
  ;(log/info "get-actor - id:" id)
  (.get stage id))

(defn create-rule [event]
  ;(log/info "*** new actor:" event)
  (-> {:id (:id event)}
      (assoc :status "new")
      (assoc :created (:created event))
      (assoc :history [event])))
(defn update-rule[actor event]
  ;(log/warn "update actor:" actor)
  ;(log/warn "update event" event)
  (-> actor
      (assoc :status "updated")
      (update :history conj event)))

(defn -serialize[this m]
  (json/encode-smile m))

(defn -deserialize[this b]
  (json/decode-smile b true))

(defn operate[rulebook event]
  (let [id (:id event)
        op (:op event)]
    (if-let [actor (get-rule rulebook id)]
      (case op
        "remove" (do
                   (.remove rulebook id)
                   (assoc event :action "removed"))
        "update" (let [updated (update-rule actor event)]
                   (.put rulebook id updated)
                   (assoc event :action "updated"))
        (assoc event :error (str "Invalid EVENT TYPE:" op)))
      (do
        (.put rulebook id (create-rule event))
        (assoc event :action "added")))))


(defn update-counter [rulebook]
  (let [aggr (.get rulebook "AGGR")]
    (if (nil? aggr)
      (.put rulebook "AGGR" {:count 1 :UUID (.toString (java.util.UUID/randomUUID))})
      (.put rulebook "AGGR" (update aggr :count inc)))))

(defn to-rawevent[event]
  (new RawEvent (:type event) (:id event) (:op event) (json/encode-smile event))
  )

(defn enrich[event rulebook]
  (-> event
      (assoc :rulebook (describe-rulebook rulebook))
      ))
(defn -flatMap1[this rawEvent collector]
  (let [smile-data (.getSmile rawEvent)
        event (json/decode-smile smile-data true)
        rulebook (.getRulebook this)
        enriched-event (enrich event rulebook)]
    (log/info "[1]" enriched-event)
    (.collect collector (to-rawevent enriched-event))))


(defn flapMap2[this rawEvent context collector]
  (let [smile-data (.getSmile rawEvent)
        event (json/decode-smile smile-data true)
        rulebook (.getRulebook this)]
    (log/info "[2]" event)
    (update-counter rulebook)
    (log/info (operate rulebook event))
    (log/info (describe-rulebook rulebook))))

(defn -processBroadcastElement[this rawEvent context collector]
  (let [smile-data (.getSmile rawEvent)
        event (json/decode-smile smile-data true)
        rulebook (.getRulebook this)]
    (log/info "[2]" event)
    (update-counter rulebook)
    (log/info (operate rulebook event))
    (log/info (describe-rulebook rulebook))))