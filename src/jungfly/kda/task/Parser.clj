(ns jungfly.kda.task.Parser
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractParser]
    :main false))

(defn -map[this value]
  (let [event (json/parse-string value true)]
    (nippy/freeze (assoc event :ingested (System/currentTimeMillis)))))