(ns jungfly.kda.task.keyed-state-test
  (:require [jungfly.kda.task.keyed-state :refer :all]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jungfly.kda.task.mock.event :as data])
  (:import (jungfly.kda.task.mock MockValueState)))

(deftest kstate-edge-test
  (let [vehicleId "1"]
    (def kstate nil)
    (def kstate (operate-kstate kstate nil))
    (is (nil? kstate))
    ))

(deftest kstate-test
  (let [vehicleId "123456789"]
    (def kstate nil)
    (testing "Testing keyed state"
      (let [event (data/parse (data/vehicle-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 1 (count (:history kstate))))
        (is (= 0 (count (:auctions kstate))))
        (is (= 0 (count (:pictures kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= "3" (:systemId kstate)))
        (is (= "13" (:vehicleStatusId kstate)))
        (is (= vehicleId (:vehicleId kstate))))
      (let [event (data/parse (data/vehicle-addtional-info-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 2 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate))))
      (let [event (data/parse (data/vehicle-addtional-info-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 0 (count (:picture kstate)))))
      (let [event (data/parse (data/picture-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:pictures kstate)))))
      (let [event (data/parse (data/auction-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:auctions kstate))))
        (is (= 1 (count (:pictures kstate)))))
      (let [event (data/parse (data/picture-update vehicleId))]
        (def kstate (operate-kstate kstate event))
        (is (= 3 (count (:history kstate))))
        (is (= "15000" (:mileage kstate)))
        (is (= "3" (:systemId kstate)))
        (is (= "13" (:vehicleStatusId kstate)))
        (is (= vehicleId (:vehicleId kstate)))
        (is (= 1 (count (:auctions kstate))))
        (is (= 2 (count (:pictures kstate))))
        (log/info "** KSTATE **" kstate))
      )))