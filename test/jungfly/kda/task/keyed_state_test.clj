(ns jungfly.kda.task.keyed-state-test
  (:require [jungfly.kda.task.keyed-state :refer :all]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.event :as data])
  (:import (jungfly.kda.task.mock MockValueState)))

(deftest enrich-test
  (let [kstate-obj (new MockValueState)
        vehicleId "123456789"]
    (testing "Testing keyed state"
      (let [event (data/parse (data/vehicle-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 1 (count (:history kstate))))
        (is (= 0 (count (:auction kstate))))
        (is (= 0 (count (:picture kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= "3" (:systemId kstate)))
        (is (= "13" (:vehicleStatusId kstate)))
        (is (= vehicleId (:vehicleId kstate))))
      (let [event (data/parse (data/vehicle-addtional-info-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 2 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate))))
      (let [event (data/parse (data/vehicle-addtional-info-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 0 (count (:picture kstate)))))
      (let [event (data/parse (data/picture-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:picture kstate)))))
      (let [event (data/parse (data/auction-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:auction kstate))))
        (is (= 1 (count (:picture kstate)))))
      (let [event (data/parse (data/picture-update vehicleId))
            result (operate-kstate kstate-obj event)
            kstate (parse-kstate kstate-obj)]
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= "3" (:systemId kstate)))
        (is (= "13" (:vehicleStatusId kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:auction kstate))))
        (is (= 2 (count (:picture kstate))))
        (log/info "** KSTATE **" kstate))
      )))