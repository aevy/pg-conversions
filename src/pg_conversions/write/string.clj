(ns pg-conversions.write.vector
  (:require [clojure.java.jdbc :as jdbc]))

(defn uuid? [s]
  (re-find #"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(extend-protocol jdbc/ISQLValue
  java.lang.String
  (sql-value [v]
    (if (uuid? v)
      (java.util.UUID/fromString v)
      v)))
