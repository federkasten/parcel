(ns parcel.core
  (:require [clojure.tools.logging :as logging]
            [parcel.mq :refer [with-connection receive-message! send-message!]]
            [parcel.config :refer [load-config! handler-config]]))

(def ^:private running-server-handler (atom nil))
(def ^:private running-server? (atom false))

(defn work
  [queue-key handler]
  (loop [msg nil]
    (when msg
      (let [task (get handler (:request msg))]
        (task (dissoc msg :request))))
    (Thread/sleep (:sleep handler-config))
    (when @running-server?
      (recur (with-connection queue-key (receive-message!))))))

(defn init!
  []
  (load-config!)
  nil)

(defn start!
  [queue-key handler]
  (when-not @running-server?
    (reset! running-server? true)
    (reset! running-server-handler (future (work queue-key handler)))
    (logging/info "Started task handler")))

(defn stop!
  []
  (when @running-server?
    (reset! running-server? false)
    (reset! running-server-handler nil)
    (logging/info "Halted task handler")))

(defn send!
  [queue-key request body]
  (with-connection queue-key
    (send-message! (assoc body :request request))))
