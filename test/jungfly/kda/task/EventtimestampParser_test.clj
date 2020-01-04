(ns jungfly.kda.task.EventtimestampParser-test
  (:require [clojure.test :refer :all])
  (:import (jungfly.kda.task EventtimestampParser)))

(deftest parse-epoch-test
  (let [t1 "2020-01-02 18:20:21.039755"]
    (def epoch (EventtimestampParser/toEpochMillis t1))
    (is (= epoch 1577989221039))
    )
  )

(deftest parse-offsetdatetime-obj-test
  (let [t1 "2020-01-02 18:20:21.039755"]
    (def dt (EventtimestampParser/toDatetime t1))
    (is (= (str dt) (.replace (str t1 "Z") " " "T")))
    )
  )

