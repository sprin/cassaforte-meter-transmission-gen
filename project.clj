(defproject cassaforte-test "0.1.0-SNAPSHOT"
  :description "A script to generate 15kHz power meter data and
               insert/aggregate into a Cassandra cluster"
  :url "https://github.com/sprin/cassaforte-meter-transmission-gen"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-time "0.5.1"]
                 [clojurewerkz/cassaforte "1.0.1"]]
  :main cassaforte-test.core)
