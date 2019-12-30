(ns jungfly.kda.task.RawParser
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractRawParser
    :exposes {outputTag {:get getOutputTag}}
    :main false)
  (:import (jungfly.kda.task RawEvent)))

(defn -processElement[this value context collector]
  (let [event (json/parse-string value true)
        type (:type event)
        id (:id event)
        op (:op event)
        smile (json/encode-smile event)
        raw (new RawEvent)]
    (log/info "raw:" type id op event)
    (.setType raw (or type "unknown"))
    (.setId raw (or id "nil"))
    (.setOp raw (or op "nil"))
    (.setSmile raw smile)

    (.collect collector raw)
    (.output context (.getOutputTag this) raw)
    ))