(ns jungfly.kda.task.LogMapFunction
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractLogMapFunction
    :exposes {name {:get getName}}
    :main false
    ))


(defn -map[this rawEvent]
  (let [type (.getType rawEvent)
        id (.getId rawEvent)
        op (.getOp rawEvent)
        event (json/decode-smile (.getSmile rawEvent) true)
        latency (- (System/currentTimeMillis) (or (:created event) 0))]
    (log/info "latency:" latency)
    (str (.getName this) ">" type "," id "," op "," latency)))