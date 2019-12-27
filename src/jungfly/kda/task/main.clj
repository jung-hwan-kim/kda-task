(ns jungfly.kda.task.main
  (:require [clojure.tools.logging :as log])
  (:gen-class
    :main true)
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (jungfly.kda.task Configurator Parser Selector Enricher)))
(defn execute[]
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
        input (.addSource env (Configurator/createSource))
        parsed (.map input (Parser.))
        keyed (.keyBy parsed (Selector.))
        enriched (.flatMap keyed (Enricher.))
        output (.addSink enriched (Configurator/createSink))]
    (.name input "in")
    (.name parsed "parser")
    (.name enriched "enricher")
    (.name output "out")
    (.execute env "KDA Prototype")))


(defn -main [& args]
  (log/info "logging in clojure")
  (execute))

