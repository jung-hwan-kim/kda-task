(ns jungfly.kda.task.Enricher
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractEnricher
    :exposes {staged {:get getStaged}}
    :main false
    ))
(defn -flatMap[this smile-data collector]
  (let [e (json/decode-smile smile-data)]
    (log/info e)
    (.collect collector (json/generate-string e))))
