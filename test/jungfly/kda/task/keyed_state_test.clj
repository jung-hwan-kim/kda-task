(ns jungfly.kda.task.keyed-state-test
  (:require [jungfly.kda.task.keyed-state :refer :all]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.data :as data])
  (:import (jungfly.kda.task.mock MockValueState)))

(deftest enrich-test
  (let [kstate-obj (new MockValueState)]
    (testing "Testing keyed state"
      (let [event (data/parse (data/vehicle-update))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 1 (count (:history kstate))))
        (log/info "** KSTATE 1 **" kstate))
      (let [event (data/parse (data/vehicle-addtional-info-update))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 2 (count (:history kstate))))
        (log/info "** KSTATE 2 **" kstate))
      )))