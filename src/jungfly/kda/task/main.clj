(ns jungfly.kda.task.main
  (:require [clojure.tools.logging :as log])
  (:gen-class
    :main true)
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (jungfly.kda.task Configurator Parser Selector KeyedEnricher OpEnricher)))
(defn execute-1[]
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
    (.execute env "KDA Prototype")))

(defn execute-2[]
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
        input (.addSource env (Configurator/createSource))
        parsed (.map input (new Parser))
       ; keyed (.keyBy parsed (new Selector))
        enriched (.flatMap parsed (new OpEnricher))
        output (.addSink enriched (Configurator/createSink))]
    (log/info (.getStateBackend env))
    (.enableCheckpointing env 1000)
    (.name input "in")
    (.name parsed "parser")
    (.name enriched "openricher")
    (.name output "out")
    (.execute env "KDA Prototype")))



(defn -main [& args]
  (log/info "Starting")
  (execute-2))

