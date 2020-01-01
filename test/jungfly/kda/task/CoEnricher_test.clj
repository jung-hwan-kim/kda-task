(ns jungfly.kda.task.CoEnricher-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (jungfly.kda.task.mock MockCollector)
           (jungfly.kda.task CoEnricher)))


(deftest a-test
  (let [f (new CoEnricher)
        collector (new MockCollector)
        rawEvent (json/encode-smile {:id "1" :type "actor"})]
    (testing "running map function"
      (let [result (.flatMap1 f rawEvent collector)
            collected (.getCollected collector)]
        (log/info collected)
        (is (= 1 (count collected))))
      (let [result (.flatMap1 f rawEvent collector)
            collected (.getCollected collector)]
        (log/info collected)
        (is (= 2 (count collected)))))))

(deftest b-test
  (let [f (new CoEnricher)
        collector (new MockCollector)
        rawEvent (json/encode-smile {:id "1" :type "rule"})]
    (testing "running map function"
      (let [result (.flatMap2 f rawEvent collector)
            collected (.getCollected collector)]
        (log/info collected)
        (is (= 0 (count collected))))
      (let [result (.flatMap1 f rawEvent collector)
            collected (.getCollected collector)]
        (log/info collected)
        (is (= 1 (count collected)))))))
