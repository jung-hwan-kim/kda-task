(ns jungfly.kda.task.InventoryEnricher
  (:require [clojure.tools.logging :as log]
            [jungfly.kda.task.keyed-state :as ks]
            [jungfly.kda.task.broadcast-state :as bs]
            [cheshire.core :as json])
  (:gen-class
    :extends jungfly.kda.task.AbstractKeyedBroadcaster
    :exposes {sideTag {:get getSideTag}}
    :main false))



(defn parse-bstate-obj[bstate-obj]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq (.entries bstate-obj)))))

(defn parse-bstate-iterable[bstate-iterable]
  (into {} (map (fn[x] {(.getKey x) (json/decode-smile (.getValue x) true)}) (seq bstate-iterable)))
  )


(defn parse-kstate-obj[kstate-obj]
  (let [v (.value kstate-obj)]
    (if (nil? v)
      nil
      (json/decode-smile v true))))

(defn update-kstate-obj[kstate-obj kstate]
  (.update kstate-obj (json/encode-smile kstate)))


(defn -process[this smile-data kstate-obj bstate-iterable collector]
  (let [event (json/decode-smile smile-data true)
        kstate (parse-kstate-obj kstate-obj)
        bstate (parse-bstate-iterable bstate-iterable)
        eventtable (:eventtable event)]
    (log/info "process:" event)
    (if (= eventtable "HEARTBEAT_K")
      (case (:action event)
        "cleanup" (do
                    (.clear kstate-obj)
                    (.collect collector (json/encode-smile (assoc event :cleaned-up kstate))))
        "kstate" (.collect collector (json/encode-smile (assoc event :kstate kstate)))
        "bstate" (.collect collector (json/encode-smile (assoc event :bstate bstate)))
        (.collect collector smile-data))
      (if-let [new-kstate (ks/transform-kstate kstate event)]
        (do
          (update-kstate-obj kstate-obj new-kstate)
          (.collect collector (json/encode-smile new-kstate)))
        (log/info "NO UPDATE")))
    (if (= (:debug event) true)
        (json/generate-string {:freeMem (/ (.freeMemory (Runtime/getRuntime)) 1024)
                               :maxMem  (/ (.maxMemory (Runtime/getRuntime)) 1024)
                               :totalMem (/ (.totalMemory (Runtime/getRuntime)) 1024)
                               :kstate (parse-kstate-obj kstate-obj) :bstate bstate}))))

(defn -processBroadcast[this smile-data bstate-obj collector]
  (let [event (json/decode-smile smile-data true)]
    (log/info "process-broadcast:" event)
    (bs/update-bstate-counter bstate-obj)
    (bs/operate-bstate bstate-obj event)
    (json/generate-string (parse-bstate-obj bstate-obj))))
