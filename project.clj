(defproject jungfly/kda-task "0.1.1"
  :description "KDA Tasks"
  :url "https://github.com/jungflykim/kda-task"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [log4j/log4j "1.2.17"]
                 [cheshire/cheshire "5.7.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [com.amazonaws/aws-kinesisanalytics-runtime "1.1.0"]
                 [com.amazonaws/aws-kinesisanalytics-flink "1.1.0"]
                 [org.apache.flink/flink-connector-kinesis_2.11 "1.8.2"]
                 [org.apache.flink/flink-streaming-java_2.11 "1.8.2"]
                 ]
  :java-source-paths ["main/java"]
  :javac-options     ["-target" "1.8" "-source" "1.8"]
  :repl-options {:init-ns jungfly.kda.task.main-test}
  :aot :all)
