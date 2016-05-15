(defproject parcel "0.1.3-SNAPSHOT"
  :description "Simple message passing library"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[com.novemberain/langohr "3.5.1" :exclusions [clj-http cheshire]]
                 [org.clojure/data.fressian "0.2.1"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}})
