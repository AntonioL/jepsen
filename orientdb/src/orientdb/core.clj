(ns orientdb.core
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [client :as client]
                    [db :as db]
                    [tests :as tests]
                    [control :as c]
                    [checker :as checker]
                    [generator :as gen]
                    [util :refer [timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [orientdb.rest :as rest]
            [clojure.data.json :as json]))

(defn from-result-to-set [res]
  (->> (:body res)
       json/read-json 
       :result
       (map :value)
       set))

(defrecord SetClient [query]
  client/Client

  (setup! [this test node]
    (let [ipaddr (name node) sess (rest/session) r (rest/connect ipaddr)]
      (assoc this :query (partial rest/query ipaddr sess))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :add (let [value (:value op) 
                   res (query (str "INSERT INTO V SET type='jepsen', value='" value "'"))]
              (assoc op 
                :type (if (= (:status res) 200) :ok :fail)))
        :read (let [res (query (str "SELECT value FROM V WHERE type='jepsen'"))]
                (if (= (:status res) 200)
                  (assoc op :type :ok :value (from-result-to-set res))
                  (assoc op :type :fail))))
      (catch Exception e (assoc op :type :fail :value (.getMessage e)))))

  (teardown! [_ test]
    (query "DELETE VERTEX V WHERE type='jepsen'")))

(defn set-client []
  (SetClient. nil))

(defn writes []
  {:type  :invoke
   :f     :add
   :value (rand-int 10000)})

(defn reads []
  {:type  :invoke
   :f     :read
   :value nil})

(defn generator []
  (gen/mix [(writes) (writes) (writes) (reads)]))

(def test-skeleton
  (assoc tests/noop-test
         :name    "OrientDB Test"
         :os      debian/os
         :db      db/noop))

