(ns jungfly.kda.task.mock.v
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(def formatter (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))
(defn inc2 [x]
  (+ x 2))
(defn to-human-readable-time [epoch-milli]
  (let [inst  (java.time.Instant/ofEpochMilli epoch-milli)
        time (java.time.LocalDateTime/ofInstant inst (java.time.ZoneId/systemDefault))]
    (.format time formatter)))

(defn gen-v[id status]
  (json/generate-string {:id id :status status :date (to-human-readable-time (System/currentTimeMillis))}))


(def init-value {:yin {:id 0 :status 0} :yang {:id 1 :status -2}})

(def v (atom init-value))

(defn init![]
  (reset! v init-value))

(defn inc-status[value]
  (-> value
      (update-in [:yin :status] inc)
      (update-in [:yang :status] inc)))

(defn rebirth[value yin?]
  (if yin?
    (-> value
        (assoc-in [:yin :status] 0)
        (update-in [:yin :id] inc2))
    (-> value
        (assoc-in [:yang :status] 0)
        (update-in [:yang :id] inc2))))

(defn change[value]
  (cond-> (inc-status value)
          (> (get-in value [:yin :status]) 2) (rebirth true)
          (> (get-in value [:yang :status]) 2) (rebirth false)))

(defn change-and-update! []
  (swap! v change)
  @v)

