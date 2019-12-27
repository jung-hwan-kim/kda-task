(ns jungfly.kda.task.Parser
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractParser]
    :main false
    ))
(defn -map[this value]
  (let [event (json/parse-string value true)]
    (log/info "map->" event)
    (json/encode-smile event)))