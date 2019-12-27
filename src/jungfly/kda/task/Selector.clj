(ns jungfly.kda.task.Selector
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:gen-class
    :implements [jungfly.kda.task.AbstractSelector]
    :main false
    ))
(defn -getKey[this smile-data]
  (let [event (json/decode-smile smile-data true)
        pkey (:id event)]
    (log/info "map->" event)
    (if (nil? pkey)
      (do
        (log/error "key was null for : " event)
        -1)
      pkey)))
