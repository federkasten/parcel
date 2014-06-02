(ns parcel
  (:require [langohr.core :as amqp-core]
            [langohr.channel :as amqp-channel]
            [langohr.basic :as amqp-basic]
            [langohr.queue :as amqp-queue]
            [clojure.data.fressian :as fressian]))

(def ^:dynamic *default-connection* (atom nil))

(defrecord Connection [config queue conn ch])

(defn open!
  [config queue]
  (let [conn (amqp-core/connect config)
        ch (amqp-channel/open conn)]
    (amqp-queue/declare ch queue :exclusive false :auto-delete true)
    (Connection. config queue conn ch)))

(defn close!
  [connection]
  (amqp-core/close (:ch connection))
  (amqp-core/close (:conn connection)))

(defn send-message!
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
  ([]
     (clear-messages! *default-connection*))
  ([connection]
     (amqp-queue/purge (:ch connection) (:queue connection))))

(defn receive-message!
  ([]
     (receive-message! *default-connection*))
  ([connection]
     (let [[{:keys [content-type type] :as meta} ^bytes payload] (amqp-basic/get (:ch connection) (:queue connection))]
       (if-not (nil? payload)
         {:body (fressian/read payload)
          :type type}))))

(defmacro with-connection
  [config queue & exprs]
  `(let [c# (parcel/open! ~config ~queue)]
     (binding [parcel/*default-connection* c#]
       ~@exprs)
     (parcel/close! c#)))
