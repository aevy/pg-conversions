(ns pg-conversions.read.object
  (:require [clojure.java.jdbc :as jdbc])
  (:import [org.postgresql.util PGobject]))

(extend-protocol jdbc/IResultSetReadColumn
  PGobject
  (result-set-read-column [pgobj metadata idx]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "jsonb" (json/read-str value :key-fn keyword)
        "json" (json/read-str value :key-fn keyword)
        :else value))))
