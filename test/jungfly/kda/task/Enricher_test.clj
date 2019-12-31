(ns jungfly.kda.task.Enricher-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (jungfly.kda.task.mock MockCollector)
           (jungfly.kda.task RawEvent Enricher)))


(deftest a-test
  (let [f (new Enricher)
        collector (new MockCollector)
        rawEvent (new RawEvent "rule" "1" "create" (json/encode-smile {:id "1" :type "TestType"}))]
    (testing "running map function"
      (let [result (.flatMap f rawEvent collector)
            collected (.getCollected collector)]
        (println collected)
        (is (= 1 (count collected)))
        )
      (let [result (.flatMap f rawEvent collector)
            collected (.getCollected collector)]
        (println collected)
        (is (= 2 (count collected)))))))

