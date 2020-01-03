(ns jungfly.kda.task.Selector
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractSelector]
    :main false
    ))
(defn -getKey[this smile-data]
  (let [event (json/decode-smile smile-data true)
        pkey (:vehicleId event)]
    (if (nil? pkey)
      (do
        (log/error "vehicleId was null for : " event)
        "nil")
      pkey)))
