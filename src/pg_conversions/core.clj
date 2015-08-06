(ns pg-conversions.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clj-time.coerce :refer [from-sql-time]])
  (:import [org.postgresql.util PGobject]))

(defn uuid? [s]
  (re-find #"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(extend-protocol jdbc/ISQLValue
  java.lang.String
  (sql-value [v]
    (if (uuid? v)
      (java.util.UUID/fromString v)
      v)))

(extend-protocol jdbc/ISQLParameter
  clojure.lang.IPersistentVector
  (set-parameter [v ^java.sql.PreparedStatement stmt ^long i]
    (let [conn (.getConnection stmt)
          meta (.getParameterMetaData stmt)
          type-name (.getParameterTypeName meta i)]
      (.setObject stmt i (cond
                          (= type-name "jsonb") (doto (PGobject.)
                                                  (.setType "jsonb")
                                                  (.setValue (json/write-str v))))))))

(extend-protocol jdbc/IResultSetReadColumn
  org.postgresql.jdbc4.Jdbc4Array
  (result-set-read-column  [pgobj metadata i]
    (vec (.getArray pgobj)))
  PGobject
  (result-set-read-column [pgobj metadata idx]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "jsonb" (json/read-str value :key-fn keyword)
        "json" (json/read-str value :key-fn keyword)
        :else value)))
  java.sql.Timestamp
  (result-set-read-column [pgobj metadata idx]
    (from-sql-time pgobj)))
