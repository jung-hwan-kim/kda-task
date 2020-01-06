(ns jungfly.kda.task.InventoryEnricher
  (:require [clojure.tools.logging :as log]
            [jungfly.kda.task.keyed-state :as ks]
            [jungfly.kda.task.broadcast-state :as bs]
            [datascript.core :as d]
            [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractKeyedBroadcaster
    :exposes {sideTag {:get getSideTag}}
    :main false))



(defn parse-bstate-obj[bstate-obj]
  (into {} (map (fn[x] {(.getKey x) (nippy/thaw (.getValue x) )}) (seq (.entries bstate-obj)))))

(defn parse-bstate-iterable[bstate-iterable]
  (into {} (map (fn[x] {(.getKey x) (nippy/thaw (.getValue x) )}) (seq bstate-iterable)))
  )


(defn parse-kstate-obj[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      nil
      (nippy/thaw v))))

(defn update-kstate-obj[kstate-obj kstate]
  (.update kstate-obj (nippy/freeze kstate)))


(defn -process[this frozen-event kstate-obj bstate-iterable collector]
  (let [event (nippy/thaw frozen-event)
        kstate (parse-kstate-obj kstate-obj)
        bstate (parse-bstate-iterable bstate-iterable)
        eventtable (:eventtable event)]
    (log/info "process:" event)
    (if (= eventtable "EDNK")
      (try
        (let [f (eval (:function event))]
          (str (f kstate bstate)))
        (catch Exception e
          (log/error e)
          (str e)))
      ;(case (:action event)
      ;  "cleanup" (do
      ;              (.clear kstate-obj)
      ;              (.collect collector (nippy/freeze (assoc event :cleaned-up kstate))))
      ;  "kstate" (.collect collector (nippy/freeze (assoc event :kstate kstate)))
      ;  "bstate" (.collect collector (nippy/freeze (assoc event :bstate bstate)))
      ;  (.collect collector frozen-event))
      (if-let [new-kstate (ks/transform-kstate kstate event)]
        (do
          (update-kstate-obj kstate-obj new-kstate)
          (.collect collector (nippy/freeze new-kstate)))
        (log/info "NO UPDATE")))
    ;(if (= (:debug event) true)
    ;    (str {:freeMem  (.freeMemory (Runtime/getRuntime))
    ;          :maxMem   (.maxMemory (Runtime/getRuntime))
    ;          :totalMem  (.totalMemory (Runtime/getRuntime))
    ;          :kstate (parse-kstate-obj kstate-obj) :bstate bstate}))
    ))

(defn -processBroadcast[this frozen-event bstate-obj collector]
  (let [event (nippy/thaw frozen-event)]
    (log/info "process-broadcast:" event)
    (bs/update-bstate-counter bstate-obj)
    (bs/operate-bstate bstate-obj event)
    (str (parse-bstate-obj bstate-obj))))
