(ns jungfly.kda.task.RawParser
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractRawParser
    :exposes {ruleTag {:get getRuleTag}
              errorTag {:get getErrorTag}}
    :main false))

(defn -processElement[this value context collector]
  (let [event (json/parse-string value true)
        type (:type event)
        smile (json/encode-smile event)]
    (log/info type)
    (case type
      "actor" (.collect collector smile)
      "rule" (do
               (log/info "rule:" event)
               (.output context (.getRuleTag this) smile)
               )
      (do
        (log/info "error:" event)
        (.output context (.getErrorTag this) smile)))))