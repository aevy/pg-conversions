(ns pg-conversions.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clj-time.coerce :refer [to-sql-time from-date from-sql-time to-string]])
  (:import [org.postgresql.util PGobject]
           [java.io PrintWriter]))

(defn write-uuid [uuid #^PrintWriter out]
  (json/-write (. uuid toString) out))

(extend java.util.UUID json/JSONWriter
  {:-write write-uuid})

(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [value]
    (to-sql-time (from-date value)))
  clojure.lang.IPersistentMap
  (sql-value [value]
    (doto (PGobject.)
      (.setType "json")
      (.setValue (json/write-str value)))))

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
                          (= type-name "json") (doto (PGobject.)
                                                 (.setType "json")
                                                 (.setValue (json/write-str v)))
                          :else
                          (if-let [elem-type (when (= (first type-name) \_) (apply str (rest type-name)))]
                            (do
                             (.createArrayOf conn elem-type (to-array (if (get #{"jsonb" "json"} elem-type)
                                                                        (map json/write-str v)
                                                                        v))))
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
