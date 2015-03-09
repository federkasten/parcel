(ns parcel
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
      (intern 'parcel '*amqp-config* (:amqp conf)))))

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
  "Send(Enqueue) a message"
  ([body type]
     (send-message! *default-connection* body type))
  ([connection body type]
     (amqp-basic/publish (:ch connection) ""
                         (:queue connection)
                         ;; (pr-str {:method method :entry entry})
                         (-> body
                             fressian/write
                             .array)
                         :content-type "application/fressian"
                         :type type)))

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
     (let [[{:keys [content-type type] :as meta} ^bytes payload] (amqp-basic/get (:ch connection) (:queue connection))]
       (if-not (nil? payload)
         {:body (fressian/read payload)
          :type type}))))

(defmacro with-connection
  ""
  [queue & exprs]
  `(let [c# (parcel/open! ~queue)]
     (binding [parcel/*default-connection* c#]
       ~@exprs)
     (parcel/close! c#)))

(load-config!)
