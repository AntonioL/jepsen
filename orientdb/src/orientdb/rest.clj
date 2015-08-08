(ns orientdb.rest
	(:require
		[clj-http.client :as http]
		[clj-http.cookies :as cookies]
		[clojure.data.json :as json]))

(def odb_user "root")
(def odb_psw "tato")
(def odb_db "Jepsen")
(def odb_port 2480)

(defn session [] (cookies/cookie-store))

(defn cmd-to-url [ip port command database extra]
      (str "http://" ip ":" port "/" command "/" database (if (> (count extra) 0) (str "/" extra) "")))

(defn connect [ip]
	(let [sess (session)]
		(http/get (cmd-to-url ip odb_port "connect" odb_db "") {:cookie-store sess :basic-auth [odb_user odb_psw]})
			sess))

(defn query [ip sess sql]
	(http/post (cmd-to-url ip odb_port "command" odb_db "sql") {:accept :json :cookie-store sess :body sql}))
