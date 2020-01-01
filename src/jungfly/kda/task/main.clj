(ns jungfly.kda.task.main
  (:require [clojure.tools.logging :as log])
  (:gen-class
    :main true)
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (jungfly.kda.task Configurator Parser Selector KeyedEnricher Enricher Broadcaster RawParser LogMapFunction)))

(defn prototype-01[]
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
        input (.addSource env (Configurator/createSource))
        parsed (.map input (new Parser))
        keyed (.keyBy parsed (new Selector))
        enriched (.flatMap keyed (new KeyedEnricher))
        output (.addSink enriched (Configurator/createSink))]
    (log/info (.getStateBackend env))
    (.name input "in")
    (.name parsed "parser")
    (.name enriched "enricher")
    (.name output "out")
    (.execute env "Prototype-1")))

;(defn prototype-02[]
;  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
;        input (.addSource env (Configurator/createSource))
;        parsed (.map input (new Parser))
;       ; keyed (.keyBy parsed (new Selector))
;        enriched (.flatMap parsed (new OpEnricher))
;        output (.addSink enriched (Configurator/createSink))]
;    (log/info (.getStateBackend env))
;    (.enableCheckpointing env 1000)
;    (.name input "in")
;    (.name parsed "parser")
;    (.name enriched "openricher")
;    (.name output "out")
;    (.execute env "Prototype-02")))

;(defn prototype-03[]
;  (let [env (Configurator/configurePrototype03 (new Parser) (new OpEnricher))]
;    (.execute env "Prototype-03")))

;(defn prototype-04[]
;  (let [env (Configurator/configurePrototype04 (new RawParser) (new LogMapFunction))]
;    (.execute env "Prototype-04")))

;(defn prototype-05[]
;  (let [env (Configurator/configurePrototype05 (new RawParser)
;                                               (new LogMapFunction)
;                                               (new LogMapFunction)
;                                               (new LogMapFunction)
;                                               (new Enricher)
;                                               (new CoEnricher))]
;    (.execute env "Prototype-05")))
(defn prototype-06[]
  (let [env (Configurator/configurePrototype06 (new RawParser)
                                               (new LogMapFunction)
                                               (new LogMapFunction)
                                               (new Enricher)
                                               (new Broadcaster))]
    (.execute env "Prototype-06")))

(defn -main [& args]
  (log/info "Starting")
  (prototype-06))

