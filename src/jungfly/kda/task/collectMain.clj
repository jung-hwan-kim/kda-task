(ns jungfly.kda.task.collectMain
  (:require [clojure.tools.logging :as log])
  (:gen-class
    :main true)
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (jungfly.kda.task Configurator SimpleCollector)))


(defn -main [& args]
  (log/info "Starting")
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)
        input (.addSource env (Configurator/createSource "ds-prototype-master"))
        collector (.map input (new SimpleCollector))
        output (.addSink collector (Configurator/createSink "ds-prototype-collect"))]
    (.name input "in")
    (.name collector "collector")
    (.name output "out")
    (.execute env "Prototype-1")))
