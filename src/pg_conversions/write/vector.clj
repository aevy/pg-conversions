(ns pg-conversions.write.vector
  (:require [clojure.java.jdbc :as jdbc]))

(extend-protocol jdbc/ISQLParameter
  clojure.lang.IPersistentVector
  (set-parameter [v ^java.sql.PreparedStatement stmt ^long i]
    (let [type-name (.getParameterTypeName (.getParameterMetaData stmt) i)]
      (.setObject stmt i (case type-name
                           "jsonb" (doto (PGobject.)
                                     (.setType "jsonb")
                                     (.setValue (json/write-str v)))
                           v)))))
