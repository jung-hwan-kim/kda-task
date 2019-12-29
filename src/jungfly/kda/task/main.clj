(ns jungfly.kda.task.main
  (:require [clojure.tools.logging :as log])
  (:gen-class
    :main true)
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (jungfly.kda.task Configurator Parser Selector Enricher OpEnricher)))
(defn execute-1[]
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
        input (.addSource env (Configurator/createSource))
        parsed (.map input (new Parser))
        keyed (.keyBy parsed (new Selector))
        enriched (.flatMap keyed (new Enricher))
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
        keyed (.keyBy parsed (new Selector))
        enriched (.flatMap keyed (new OpEnricher))
        output (.addSink enriched (Configurator/createSink))]
    (log/info (.getStateBackend env))
    (.name input "in")
    (.name parsed "parser")
    (.name enriched "openricher")
    (.name output "out")
    (.execute env "KDA Prototype")))



(defn -main [& args]
  (log/info "Starting")
  (execute-2))

