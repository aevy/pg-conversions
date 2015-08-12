(ns pg-conversions.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clj-time.coerce :refer [to-sql-time from-date from-sql-time to-string]])
  (:import [org.postgresql.util PGobject]))

(defn uuid? [s]
  (re-find #"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [value]
    (prn "oeuoehnuthaoeuntoheuntaoehusntoehuntoehusntoheauntoheausnhoeauntaoehunstaoehuntsosh")
    (prn (to-sql-time (from-date value)))
    (to-sql-time (from-date value)))
  clojure.lang.IPersistentMap
  (sql-value [value]
    (doto (PGobject.)
      (.setType "jsonb")
      (.setValue (json/write-str value))))
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
                                                  (.setValue (json/write-str v)))
                          :else
                          (if-let [elem-type (when (= (first type-name) \_) (apply str (rest type-name)))]
                            (.createArrayOf conn elem-type (to-array v))
                            v))))))

(extend-protocol jdbc/IResultSetReadColumn
  java.util.UUID
  (result-set-read-column [pgobj _ _]
    (str pgobj))
  org.postgresql.jdbc4.Jdbc4Array
  (result-set-read-column  [pgobj _ _]
    (vec (.getArray pgobj)))
  PGobject
  (result-set-read-column [pgobj _ _]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "jsonb" (json/read-str value :key-fn keyword)
        "json" (json/read-str value :key-fn keyword)
        :else value)))
  java.sql.Timestamp
  (result-set-read-column [pgobj _ _]
    (to-string (from-sql-time pgobj))))
