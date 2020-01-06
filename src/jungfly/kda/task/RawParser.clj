(ns jungfly.kda.task.RawParser
  (:require [clojure.tools.logging :as log]
            [camel-snake-kebab.core :as csk]
            [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractRawParser
    :exposes {ruleTag  {:get getRuleTag}
              errorTag {:get getErrorTag}}
    :main false)
  (:import (java.util Base64)))

(defn encode-base64str[edn]
  (.encodeToString (Base64/getEncoder) (nippy/freeze edn)))

(defn decode-edn [base64-str]
  (nippy/thaw (.decode (Base64/getDecoder) base64-str)))

(defn decode [event]
  (.decode (Base64/getDecoder) (:edn event)))

(defn -processElement[this json-str context collector]
    (try
      (let [event (json/parse-string json-str (fn[x] (keyword (csk/->camelCase x))))
            table (:eventtable event)]
        (case table
          ("ADLOAD.VEHICLES" "ADLOAD.VEHICLE_ADDITIONAL_INFOS" "ADLOAD.CURRENT_AUCTIONS" "ADLOAD.PICTURES") (.collect collector (nippy/freeze event))
          "rule" (.output context (.getRuleTag this) (nippy/freeze event))
          "EDNB" (.output context (.getRuleTag this) (decode event))
          "EDNK" (.collect collector (decode event))
          (do
            (log/error "unsupported table name:" table)
            (.output context (.getErrorTag this) (str "Unsupported TABLE:" table)))))
      (catch Exception e
        (log/error (.getMessage e))
        (log/error json-str)
        )))
