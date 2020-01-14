(ns jungfly.kda.task.SimpleCollector
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractCollector]
    :main false))

(defn -map[this value]
  (let [event (json/parse-string value true)
        ingested (:ingested event)
        dur (:dur event)]
    (json/generate-string (assoc event :dur (- (System/currentTimeMillis) ingested) :dur_orig dur))))