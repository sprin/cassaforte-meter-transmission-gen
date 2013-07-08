(defproject cassaforte-test "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-time "0.5.1"]
                 [cc.qbits/hayt "1.1.2"
                  :exclusions [org.flatland/useful]]
                 [com.datastax.cassandra/cassandra-driver-core "1.0.1"]]
  :plugins [[lein-checkouts "1.1.0"]]
  :main cassaforte-test.core)
