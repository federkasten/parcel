(defproject parcel "0.1.2-SNAPSHOT"
  :description "Simple message passing libbrary using AMQP(RabbitMQ)"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[com.novemberain/langohr "3.5.0" :exclusions [clj-http cheshire]]
                 [org.clojure/data.fressian "0.2.1"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}})
