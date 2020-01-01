(ns jungfly.kda.task.LogMapFunction
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractLogMapFunction
    :exposes {name {:get getName}}
    :main false
    ))


(defn -map[this rawEvent]
  (let [event (json/decode-smile rawEvent true)
        name (.getName this)]
    (log/info name event)
    (json/generate-string (assoc event :logged (System/currentTimeMillis)))))