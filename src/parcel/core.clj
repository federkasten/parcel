(ns parcel.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.data.fressian :as fressian]
            [langohr.core :as amqp-core]
            [langohr.channel :as amqp-channel]
            [langohr.basic :as amqp-basic]
            [langohr.queue :as amqp-queue]))

(def ^:private config-filename "parcel.config")

(def ^:dynamic *default-connection* nil)

(def ^:dynamic *amqp-config* nil)

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
      (intern 'parcel.core '*amqp-config* (:amqp conf)))))

(defrecord Connection [config queue conn ch])

(defn open!
  "Open a connection to a quque"
  ([queue]
   (if-let [config *amqp-config*]
     (open! config queue)
     (throw (RuntimeException. (str "AMQP configuration is not specified")))))
  ([config queue]
   (let [conn (amqp-core/connect config)
         ch (amqp-channel/open conn)]
     (amqp-queue/declare ch queue {:exclusive false :auto-delete true})
     (Connection. config queue conn ch))))

(defn close!
  "Close a connection from a queue"
  [connection]
  (amqp-core/close (:ch connection))
  (amqp-core/close (:conn connection)))

(defn send-message!
  "Send a message"
  ([body]
   (send-message! *default-connection* body))
  ([connection body]
   (amqp-basic/publish (:ch connection)
                       ""
                       (:queue connection)
                       (-> body
                           fressian/write
                           .array)
                       {:content-type "application/fressian"})))

(defn clear-messages!
  "Clear all messages from a queue"
  ([]
   (clear-messages! *default-connection*))
  ([connection]
   (amqp-queue/purge (:ch connection) (:queue connection))))

(defn receive-message!
  "Receive one message from a queue"
  ([]
   (receive-message! *default-connection*))
  ([connection]
   (let [[{:keys [content-type] :as meta} ^bytes payload] (amqp-basic/get (:ch connection) (:queue connection))]
     (if-not (nil? payload)
       (fressian/read payload)))))

(defmacro with-connection
  ""
  [queue & exprs]
  `(let [c# (parcel.core/open! ~queue)]
     (binding [parcel.core/*default-connection* c#]
       (let [result# (do ~@exprs)]
         (parcel.core/close! c#)
         result#))))
