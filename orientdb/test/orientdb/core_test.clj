(ns orientdb.core-test
  (:require [clojure.test :refer :all]
            [orientdb.core :as orientdb]
            [jepsen [core      :as jepsen]
                    [util      :as util]
                    [checker   :as checker]
                    [model     :as model]
                    [tests     :as tests]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]]))

(deftest a-test
  (let [test (jepsen/run! (orientdb/test-linearizability))]
  	(is (:valid? (:results test)))))
