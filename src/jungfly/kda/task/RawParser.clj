(ns jungfly.kda.task.RawParser
  (:require [clojure.tools.logging :as log]
            [camel-snake-kebab.core :as csk]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractRawParser
    :exposes {ruleTag {:get getRuleTag}
              errorTag {:get getErrorTag}}
    :main false))

(defn -processElement[this value context collector]
    (try
       (let [event (json/parse-string value (fn[x] (keyword (csk/->camelCase x))))
             table (:eventtable event)
             smile (json/encode-smile event)]
         (case table
           ("ADLOAD.VEHICLES" "ADLOAD.VEHICLE_ADDITIONAL_INFOS" "ADLOAD.CURRENT_AUCTIONS" "ADLOAD.PICTURES") (.collect collector smile)
           "rule" (do
                    (log/info "rule:" event)
                    (.output context (.getRuleTag this) smile)
                    )
           (do
             (log/error "unsupported table name:" table)
             (.output context (.getErrorTag this) smile))))
       (catch Exception e
         (log/error value)
         (log/error (.getMessage e))
         )))
