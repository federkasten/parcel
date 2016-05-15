(ns parcel.mq
  (:require [langohr.core :as amqp-core]
            [langohr.channel :as amqp-channel]
            [langohr.basic :as amqp-basic]
            [langohr.queue :as amqp-queue]
            [parcel.entry :as entry]))

(def ^:dynamic *default-connection* nil)

(defrecord Connection [spec queue conn ch])

(defn open!
  "Open a connection to a quque"
  [queue-spec queue]
  (let [conn (amqp-core/connect queue-spec)
        ch (amqp-channel/open conn)]
    (amqp-queue/declare ch queue {:exclusive false :auto-delete true})
    (Connection. queue-spec queue conn ch)))

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
                       (entry/encode body)
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
       (entry/decode payload)))))

(defmacro with-connection
  ""
  [queue-spec queue & exprs]
  `(let [c# (parcel.mq/open! ~queue-spec ~queue)]
     (binding [parcel.mq/*default-connection* c#]
       (let [result# (do ~@exprs)]
         (parcel.mq/close! c#)
         result#))))
