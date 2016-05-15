(ns parcel.core
  (:require [parcel.mq :as queue :refer [with-connection]]))

(defn dispatch!
  [queue-spec queue-key {:keys [request body] :as msg}]
  (with-connection queue-spec queue-key
    (queue/send-message! {:request request
                          :body body})))

(defn start-server!
  ([queue-spec queue-key handlers]
   (start-server! queue-spec queue-key handlers {}))
  ([queue-spec queue-key handlers {:keys [interval]
                                   :or {interval 1000}
                                   :as opts}]
   (let [available? (atom true)]

     (future
       (while @available?
         (try
           (when-let [msg (with-connection queue-spec queue-key (queue/receive-message!))]
             (when-let [handler-fn (get handlers (:request msg))]
               (handler-fn (:body msg))))
           (catch Exception e (do (.printStackTrace e)
                                  nil)))
         (Thread/sleep interval)))

     ;; return server instance
     {:queue-key queue-key
      :available? available?
      :handlers handlers
      :opts opts})))

(defn stop-server!
  [server]
  (reset! (:available? server) false)
  nil)
