(ns jungfly.kda.task.LogMapFunction
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractLogMapFunction
    :exposes {name {:get getName}}
    :main false
    ))


(defn -map[this rawEvent]
  (let [event (nippy/thaw rawEvent)
        ingested (:ingested event)]
    (json/generate-string (assoc event :dur (- (System/currentTimeMillis) ingested)))))