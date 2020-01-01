(ns jungfly.kda.task.mock.MockCollector
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.mock.AbstractMockCollector
    :state state
    :init init
    :main false
    :methods [[getCollectedRaw[] Object]
              [getCollected[] Object]]
    ))

(defn -init[]
  [[] (atom [])])

(defn -getCollectedRaw[this]
  @(.state this))

(defn -getCollected[this]
  (map (fn[x] (json/decode-smile x true)) @(.state this)))

(defn -collect [this rawEvent]
  (swap! (.state this) conj rawEvent))



