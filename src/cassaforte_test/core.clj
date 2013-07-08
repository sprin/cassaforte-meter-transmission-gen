(ns cassaforte-test.core
  (:require [clojurewerkz.cassaforte.client :as client]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query)
  (:import (java.security MessageDigest))
  (:gen-class))


;; The sample rate of meters in Hz, and the number of samples contained in
;; a meter transmission.
(def SAMPLE_RATE 15000)

;; Number of meters sending in transmissions.
(def NUM_METERS 100)

(defn select-host-id
  "Select the host_id from the Cassandra instance."
  []
  (use-keyspace "system")
  (str (select :local
          (columns :host_id))))

(defn host-hash
  "An integer obtained from hashing the host id. To be used to ensure that
  this script will generate different SAIDs on different hosts, but always
  generate the same SAID on the same host."
  []
  (let [bytes (.getBytes (select-host-id))
        sha1 (.digest (MessageDigest/getInstance "SHA1") bytes)
        sha1_bigint (new java.math.BigInteger sha1)]
    (int (mod sha1_bigint 100000))))

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

(defn hhmmss
  "Get time now in hh:mm:ss (UTC)"
  [datetime]
  (tformat/unparse (tformat/formatter "HH:mm:ss z") datetime))

(defn do-samples-inserts
  "Take the data for one meter transmission and insert it into the raw and
  aggregation tables. This spawns another thread with `future` so the calls are
  non-blocking. We do not need the results of the inserts/updates, so there is
  no return values. Uses prepared statements for efficiency."
  [said datetime samples]
  (future
    (let [joules (joules-over-second samples)]
      (println (format "\nInserting transmission (%s, %s)"
                       said (hhmmss datetime)))
      ;; Insert raw, 15 kHz meter samples.
      (insert :meter_samples
        {
          :said said
          :datetime (tcoerce/to-date datetime)
          :watts samples
        })
      ;; Aggregate on second intervals.
      (insert :meter_samples_second
        {
         :said said
         :datetime (tcoerce/to-date datetime)
         :joules joules
        })
      ;; Increase counter on minute aggregation table.
      (update :meter_samples_minute
        {:joules (increment-by (long joules))}
        (where
          :said said
          :datetime (tcoerce/to-date (trunc-to-min datetime))
        ))
      ;; Increase counter on hour aggregation table.
      (update :meter_samples_hour
        {:joules (increment-by (long joules))}
        (where
          :said said
          :datetime (tcoerce/to-date (trunc-to-hour datetime))
        ))
      ;; Increase counter on day aggregation table.
      (update :meter_samples_day
        {:joules (increment-by (long joules))}
        (where
          :said said
          :datetime (tcoerce/to-date (trunc-to-day datetime))
        ))
      (println (format "\nDone inserting transmission (%s, %s)"
                       said (hhmmss datetime))))))

(defn generate-samples
  "Generate samples for the given time and call the insert function.
  Returns a seq of futures to be deref'd later."
  [start-said datetime]
    (let [samples (take SAMPLE_RATE watts)]
        (map
          #(do-samples-inserts (int %) datetime samples)
          (range start-said (+ NUM_METERS start-said)))))

(defn -main
  "Connect to Cassandra and begin inserting meter transmissions every 1 second."
  [& args]
  (client/connect! ["127.0.0.1"] :force-prepared-queries true)
  (let [start-said (host-hash)]
    (println start-said)
    (use-keyspace "disagg")
    (while true
      (let
        [futures (generate-samples start-said (tcore/now))]
        ;; Sleep for 1s, then deref futures.
        (Thread/sleep 1000)
        (doall (map deref futures))))))

