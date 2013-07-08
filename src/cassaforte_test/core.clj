(ns cassaforte-test.core
  (:require [clojurewerkz.cassaforte.client :as client]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query))

;; The sample rate of meters in Hz, and the number of samples contained in
;; a meter transmission.
(def SAMPLE_RATE 15000)

(def datetimes
  "seq of every second as Joda DateTime starting from 2013-01-01 00:00:00"
  (map tcoerce/from-long
       (iterate (fn [x] (+ x 1000)) 1357027200000)))

(def watts
  "seq of mock real power values in watts."
  (map float (cycle (range 0 120))))

(defn joules-over-second
  "Sum the power sample array (assumed over one second) to get joules."
  [samples]
  (int (/ (reduce + 0 samples) SAMPLE_RATE)))

(defn trunc-to-min
  "Truncate a Joda DateTime to the minute."
  [datetime]
  (.roundFloorCopy (.minuteOfDay datetime)))

(defn trunc-to-hour
  "Truncate a Joda DateTime to the hour"
  [datetime]
  (.roundFloorCopy (.hourOfDay datetime)))

(defn trunc-to-day
  "Truncate a Joda DateTime to the day"
  [datetime]
  (.roundFloorCopy (.dayOfMonth datetime)))

(defn time-hhmmss
  "Get time now in hh:mm:ss (UTC)"
  []
  (tformat/unparse (tformat/formatter "HH:mm:ss z") (tcore/now)))

(defn do-samples-inserts
  "Take the data for one meter transmission and insert it into the raw and
  aggregation tables. This spawns another thread with `future` so the calls are
  non-blocking. We do not need the results of the inserts/updates, so there is
  no return values. Uses prepared statements for efficiency."
  [said datetime samples]
  (future
    (let [joules (joules-over-second samples)]
        (println "Begin insert")
        (client/prepared
          (insert :meter_samples
            {
              :said said
              :datetime (tcoerce/to-date datetime)
              :watts samples
            }))
        ;; Aggregate on 1s intervals
        (client/prepared
          (insert :meter_samples_second
            {
             :said said
             :datetime (tcoerce/to-date datetime)
             :joules joules
            }))
        ;; Increase counter on 1m aggregation table
        (client/prepared
          (update :meter_samples_minute
            {:joules (increment-by (long joules))}
            (where
              :said said
              :datetime (tcoerce/to-date (trunc-to-min datetime))
            )))
        (client/prepared
          (update :meter_samples_hour
            {:joules (increment-by (long joules))}
            (where
              :said said
              :datetime (tcoerce/to-date (trunc-to-hour datetime))
            )))
        (client/prepared
          (update :meter_samples_day
            {:joules (increment-by (long joules))}
            (where
              :said said
              :datetime (tcoerce/to-date (trunc-to-day datetime))
            )))
      (println "Done inserting"))))

(defn generate-samples
  "Generate samples for the given time and call the insert function."
  [datetime]
    (let [samples (take SAMPLE_RATE watts)
          said (int 1)]
      (do-samples-inserts said datetime samples)))

(defn -main
  "Connect to Cassandra and begin inserting meter transmissions every 1 second."
  [& args]
  (client/connect! ["127.0.0.1"])
  (use-keyspace "disagg")
  (doseq [datetime (take 120 datetimes)]
    (generate-samples datetime)
    (println (time-hhmmss))
    (Thread/sleep 1000)))

