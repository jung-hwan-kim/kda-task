(ns jungfly.kda.task.LogMapFunction-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json])
  (:import (jungfly.kda.task LogMapFunction)))

(deftest a-test
  (let [f (new LogMapFunction)
        rawEvent (json/encode-smile {:id "1" :type "test"})]
    (.name f "TEST-LOG")
    (testing "running map function"
      (let [result (.map f rawEvent)]
        (is (not (nil? result)))
        (is (string? result))
        ))))