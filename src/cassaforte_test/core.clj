(ns cassaforte-test.core
  (:require [clojurewerkz.cassaforte.client :as client])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query))


;; Microseconds starting from 2013-01-01
(def datetimes (iterate inc 1357027260000000))

(defn timenow [] (new java.util.Date))

;; Table creation:
;; create table meter_samples
;; (said int, datetime bigint, volts float, amps float,
;; PRIMARY KEY (said, datetime));

(defn -main
  [& args]
  (client/connect! ["127.0.0.1"])
  (use-keyspace "disagg")
  (time (doseq [dt (take 10000 datetimes)]
    (client/execute
        ["INSERT INTO meter_samples (said, datetime, volts, amps)
        VALUES (?, ?, ?, ?);"
        [(int 1) (long dt) (float 120) (float 1)]]
      :prepared true))))
