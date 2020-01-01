(ns jungfly.kda.task.Broadcaster
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractBroadcaster
    :exposes {ruleStateDescriptor {:get getRuleStateDescriptor}}
    :main false))

(defn describe-node[node]
  (let [key (.getKey node)
        value (.getValue node)]
    {:key key :value value}))

(defn describe-rulebook[rulebook]
  (map describe-node (iterator-seq (.iterator rulebook))))

(defn new-state-value[event]
  ;(log/info "*** new actor:" event)
  (-> {:id (:id event)}
      (assoc :status "new")
      (assoc :created (System/currentTimeMillis))
      (assoc :history [event])))
(defn updated-state-value[value event]
  (-> value
      (assoc :status "updated")
      (assoc :updated (System/currentTimeMillis))
      (update :history conj event)))

(defn get-value[state id]
  (json/decode-smile (.get state id)))

(defn operate[state event]
  (let [id (:id event)
        op (:op event)]
    (if-let [state-value (get-value state id)]
      (case op
        "remove" (do
                   (.remove state id))
        "update" (let [updated (updated-state-value state-value event)]
                   (.put state id (json/encode-smile updated)))
        (assoc event :error (str "Invalid EVENT TYPE:" op)))
      (do
        (.put state id (json/encode-smile (new-state-value event)))
        (assoc event :action "added")))))

(defn inc-counter-smile [smile]
  (if (nil? smile)
    (let [aggr {:count 1 :UUID (.toString (java.util.UUID/randomUUID))}]
      (log/info aggr)
      (json/encode-smile aggr))
    (let [aggr (update (json/decode-smile smile true) :count inc)]
      (log/info aggr)
      (json/encode-smile aggr))))

(defn update-counter [state]
  (let [aggr (inc-counter-smile (.get state "aggr"))]
    (.put state "aggr" aggr)))


(defn describe-state[state]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq (.entries state)))))

(defn describe-state-iterable[state-iterable]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq state-iterable)))
  )

(defn enrich[event state-iterable]
  (-> event
      (assoc :rules (describe-state-iterable state-iterable))
      ))

(defn -process[this smile-data state-iterable collector]
  (let [event (json/decode-smile smile-data true)]
    (log/info "[1]" event)
    (.collect collector (json/encode-smile (enrich event state-iterable)))))

(defn -processBroadcast[this smile-data state collector]
  (let [event (json/decode-smile smile-data true)]
    (log/info "[broadcast]" event)
    (update-counter state)
    (log/info "OP "(operate state event))
    ;(log/info "STATE " (describe-state state))
    ))