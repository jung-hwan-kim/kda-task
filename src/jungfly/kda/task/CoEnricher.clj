(ns jungfly.kda.task.CoEnricher
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractCoEnricher
    :exposes {rulebook {:get getRulebook}}
    :main false
    )
  (:import (jungfly.kda.task RawEvent)))

(defn describe-node[node]
  (let [key (.getKey node)
        value (.getValue node)]
    {:key key :value value}))

(defn describe-rulebook[rulebook]
  (map describe-node (iterator-seq (.iterator (.entrySet rulebook)))))

(defn -serialize[this m]
  (json/encode-smile m))

(defn -deserialize[this b]
  (json/decode-smile b true))

(defn update-counter [rulebook]
  (let [aggr (.get rulebook "AGGR")]
    (if (nil? aggr)
      (.put rulebook "AGGR" {:count 1 :UUID (.toString (java.util.UUID/randomUUID))})
      (.put rulebook "AGGR" (update aggr :count inc)))))


(defn -flatMap1[this rawEvent collector]
  (let [smile-data (.getSmile rawEvent)
        event (json/decode-smile smile-data true)
        rulebook (.getRulebook this)]
    (log/info "[1]" event)

    (.collect collector rawEvent)))

(defn -flatMap2[this rawEvent collector]
  (let [smile-data (.getSmile rawEvent)
        event (json/decode-smile smile-data true)
        rulebook (.getRulebook this)]
    (log/info "[2]" event)
    (update-counter rulebook)
    (log/info (describe-rulebook rulebook))))