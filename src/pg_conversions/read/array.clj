(ns pg-conversions.write.vector
  (:require [clojure.java.jdbc :as jdbc]))

(extend-protocol jdbc/IResultSetReadColumn
  org.postgresql.jdbc4.Jdbc4Array
  (result-set-read-column  [pgobj metadata i]
    (vec (.getArray pgobj))))
