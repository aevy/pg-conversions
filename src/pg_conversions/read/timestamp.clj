(ns pg-conversions.read.timestamp
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.coerce :refer [from-sql-time]]))

(extend-protocol jdbc/IResultSetReadColumn
  java.sql.Timestamp
  (result-set-read-column [pgobj metadata idx]
    (from-sql-time pgobj)))
