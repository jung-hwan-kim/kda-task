(ns jungfly.kda.task.keyed-state-test
  (:require [jungfly.kda.task.keyed-state :refer :all]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.data :as data])
  (:import (jungfly.kda.task.mock MockValueState)))

(deftest enrich-test
  (let [ks (new MockValueState)
        data (data/parse (data/vehicle-update))]
    (testing "Testing keyed state"
      (let [kstate (parse-kstate ks)]
        (log/info collected)
        (is (= 1 (count collected)))
        (println kstate))
      (let [ collected (do-process2 f ks bs c data)
            bstate (parse-bstate bs)
            kstate (parse-kstate ks)]
        (log/info collected)
        (is (= 2 (count collected)))
        (println "** KSTATE **" kstate)
        kstate)
      )))