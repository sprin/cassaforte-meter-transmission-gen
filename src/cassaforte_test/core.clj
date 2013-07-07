(ns cassaforte-test.core
  (:require [clojurewerkz.cassaforte.client :as client]
            [clj-time.core :as cljt]
            [clj-time.coerce :as coerce])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query))

(def SAMPLE_RATE 15000)

;; seq of every second as a Date starting from 2013-01-01 00:00:00
(def datetimes (map coerce/from-long
                    (iterate (fn [x] (+ x 1000)) 1357027200000)))

;; seq of mock real power values in watts.
(def watts (map float (cycle (range 0 120))))

(defn joules-over-second
  [samples]
  (int (/ (reduce + 0 samples) SAMPLE_RATE)))

(defn trunc-to-min [datetime] (.roundFloorCopy (.minuteOfDay datetime)))

(defn trunc-to-hour [datetime] (.roundFloorCopy (.hourOfDay datetime)))

(defn trunc-to-day [datetime]
  (.roundFloorCopy (.dayOfMonth datetime)))


(defn do-samples-inserts
  [said datetime samples]
  (let [joules (joules-over-second samples)]
      (time (client/prepared
        (insert :meter_samples
          {
            :said said
            :datetime (coerce/to-date datetime)
            :watts samples
          })))
      ;; Aggregate on 1s intervals
      (time (client/prepared
        (insert :meter_samples_second
          {
           :said said
           :datetime (coerce/to-date datetime)
           :joules joules
          })))
      ;; Increase counter on 1m aggregation table
      (time (client/prepared
        (update :meter_samples_minute
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (coerce/to-date (trunc-to-min datetime))
          ))))
      (time (client/prepared
        (update :meter_samples_hour
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (coerce/to-date (trunc-to-hour datetime))
          ))))
      (time (client/prepared
        (update :meter_samples_day
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (coerce/to-date (trunc-to-day datetime))
          ))))))

(defn -main
  [& args]
  (client/connect! ["127.0.0.1"])
  (use-keyspace "disagg")
  (time (doseq [datetime (take 120 datetimes)]
    (let [samples (take SAMPLE_RATE watts)
          said (int 1)]
      (do-samples-inserts said datetime samples)))))

