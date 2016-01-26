(ns parcel.config
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]))

(def ^:private config-filename "parcel.config")

(def ^:dynamic amqp-config nil)

(def handler-config {:sleep 500})

(defn load-config!
  "Load configuration file from class path"
  []
  (let [rsrc (if-let [r (io/resource (str config-filename ".clj"))]
               r
               (if-let [r (io/resource (str config-filename ".edn"))]
                 r
                 nil))]
    (when-let [conf (if-not (nil? rsrc)
                      (edn/read-string (slurp rsrc)))]
      (intern 'parcel.config 'amqp-config (:amqp conf))
      (intern 'parcel.config 'handler-config (:handler conf)))))
