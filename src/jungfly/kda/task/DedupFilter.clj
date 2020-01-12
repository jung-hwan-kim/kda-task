(ns jungfly.kda.task.DedupFilter
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy])
  (:gen-class
    :extends jungfly.kda.task.AbstractFilter
    :exposes {state {:get getState}}
    :main false
    ))

(defn get-state[this]
  (let [state-value  (.value (.getState this))]
    (if (nil? state-value)
      {}
      (nippy/thaw state-value))))

(defn update-state[this state]
  (let [state-obj (.getState this)]
    (.update state-obj (nippy/freeze state))))

(defn clear-state [this]
  (.clear (.getState this)))

(defn -filter[this frozen-event]
  (let [event (nippy/thaw frozen-event)
        eventtype (:eventtype event)
        state (get-state this)
        content-of-event (dissoc event :eventtype :id :eventdate :ingested)]
    (if (= state content-of-event)
      (do
        (log/info "Removing duplicated event of " eventtype)
        false)
      (do
        (log/info "OK:" eventtype)
        (update-state this content-of-event)
        true))))