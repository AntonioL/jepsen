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
                    [model     :as model]
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

(defn w []
  {:type  :invoke
   :f     :add
   :value (rand-int 10000)})

(defn r []
  {:type :invoke :f :read})

(defn generator []
  (gen/mix [(w) (w) (w) (r)]))

;Shamefully almost copied from MongoDB test
(defn std-gen [gen]
  (gen/phases
    (->> gen
         (gen/delay 1)
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 60)
                            {:type :info :f :stop}
                            {:type :info :f :start}])))
         (gen/time-limit 300))
    (gen/nemesis
      (gen/once {:type :info :f :stop}))
    (gen/clients
      (gen/once (r)))))

(defn test-skeleton [name]
  (assoc tests/noop-test
         :name    (str "orientdb " name) 
         :os      debian/os
         :db      (db/noop)))

(def test-linearizability
  (assoc (test-skeleton "linearizability test 1")
    :model      (model/set)
    :client     (set-client)
    :checker    (checker/set)
    :generator  (std-gen (gen/mix [(w) (w) (w) (r)]))
    :nemesis    (nemesis/partition-halves)))