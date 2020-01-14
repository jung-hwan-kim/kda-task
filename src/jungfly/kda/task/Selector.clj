(ns jungfly.kda.task.Selector
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractSelector]
    :main false
    ))
(defn -getKey[this smile-data]
  (let [event (nippy/thaw smile-data)
        pkey (:vehicleId event)]
    (if (nil? pkey)
      (do
        (log/error "vehicleId was null for : " event)
        "nil")
      (str pkey))))
