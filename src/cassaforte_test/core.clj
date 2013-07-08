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

;; Default number of meters sending in transmissions.
;; Can be set as a command-line arg.
(def NUM_METERS_DEFAULT 2)

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
  "seq of mock real power values in watts.
  TODO: Generate more interesting values, possilby using SAID/datetime
  as input."
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

(defn get-success-handler
  "Return a handler which will print a success message with the SAID, datetime.
  Does not print a succes message until called num_calls times."
  [num_calls said datetime]
  (let [counter (atom 0)]
    (fn [r] (do
      (swap! counter inc)
      (if
        (= @counter num_calls)
        (println (format "\nSuccess inserting transmission (%s, %s) (%s)"
                         said (hhmmss datetime) @counter)))))))

(defn get-failure-handler
  [said datetime]
  "Return a handler to print a success message with the SAID, datetime."
    (fn [r]
      (println (format "\nFailed one insert (%s, %s)"
                       said (hhmmss datetime)))))

(defn do-samples-inserts
  "Take the data for one meter transmission and insert it into the raw and
  aggregation tables. This spawns another thread with `future` so the calls are
  non-blocking. We do not need the results of the inserts/updates, so there is
  no return values. Uses prepared statements for efficiency."
  [said datetime samples]
  (let [joules (joules-over-second samples)
        num_queries 5
        success-handler (get-success-handler num_queries said datetime)
        failure-handler (get-failure-handler said datetime)]
    (println (format "\nInserting transmission (%s, %s)"
                     said (hhmmss datetime)))
    ;; Insert raw, 15 kHz meter samples.
    (client/set-callbacks
      (client/async
        (insert :meter_samples
          {
            :said said
            :datetime (tcoerce/to-date datetime)
            :watts samples
          }))
        :success success-handler
        :failure failure-handler)
    ;; Aggregate on second intervals.
    (client/set-callbacks
      (client/async
        (insert :meter_samples_second
          {
           :said said
           :datetime (tcoerce/to-date datetime)
           :joules joules
          }))
      :success success-handler
      :failure failure-handler)
    ;; Increase counter on minute aggregation table.
    (client/set-callbacks
      (client/async
        (update :meter_samples_minute
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (tcoerce/to-date (trunc-to-min datetime))
          )))
      :success success-handler
      :failure failure-handler)
    ;; Increase counter on hour aggregation table.
    (client/set-callbacks
      (client/async
        (update :meter_samples_hour
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (tcoerce/to-date (trunc-to-hour datetime))
          )))
      :success success-handler
      :failure failure-handler)
    ;; Increase counter on day aggregation table.
    (client/set-callbacks
      (client/async
        (update :meter_samples_day
          {:joules (increment-by (long joules))}
          (where
            :said said
            :datetime (tcoerce/to-date (trunc-to-day datetime))
          )))
      :success success-handler
      :failure failure-handler)))

(defn generate-samples
  "Generate samples for the given time and call the insert function."
  [num_meters start-said datetime]
    (let [samples (take SAMPLE_RATE watts)]
        (doall (map
          #(do-samples-inserts (int %) datetime samples)
          (range start-said (+ num_meters start-said))))))

(defn -main
  "Connect to Cassandra and begin inserting meter transmissions every 1 second."
  ([]
    (-main NUM_METERS_DEFAULT))

  ([num_meters]
    (client/connect! ["127.0.0.1"] :force-prepared-queries true)
    (let [start-said (host-hash)]
      (println start-said)
      (use-keyspace "disagg")
      (while true
          (generate-samples (new Integer num_meters) start-said (tcore/now))
          ;; Sleep for 1s before re-entering loop.
          (Thread/sleep 1000)))))

