(ns jungfly.kda.task.RawParser-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.event :as data])
  (:import (jungfly.kda.task.mock MockCollector MockBroadcastState MockValueState)
           (jungfly.kda.task RawParser)
           ))
(deftest b-test
  (let [d (json/generate-string (data/vehicle-update))
        f (new RawParser)
        c (new MockCollector)]
    (log/info d)
    (.processElement f d nil c)
    (is (= 1 1))
    ))
