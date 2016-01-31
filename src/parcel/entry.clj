(ns parcel.entry
  (:require [clojure.data.fressian :as fressian])
  (:import [org.fressian.handlers ReadHandler WriteHandler]))

(def ^:private collection-write-handlers
  {clojure.lang.PersistentList
   {"list"
    (reify WriteHandler
      (fressian/write [_ w s]
        (.writeTag w "list" 1)
        (.writeList w s)))}

   clojure.lang.PersistentVector
   {"vector"
    (reify WriteHandler
      (fressian/write [_ w s]
        (.writeTag w "vector" 1)
        (.writeList w s)))}

   clojure.lang.PersistentHashSet
   {"hashset"
    (reify WriteHandler
      (fressian/write [_ w s]
        (.writeTag w "hashset" 1)
        (.writeList w s)))}})

(def ^:private collection-read-handlers
  {"list"
   (reify ReadHandler
     (fressian/read [_ rdr tag component-count]
       (apply list (.readObject rdr))))

   "vector"
   (reify ReadHandler
     (fressian/read [_ rdr tag component-count]
       (vec (.readObject rdr))))

   "hashset"
   (reify ReadHandler
     (fressian/read [_ rdr tag component-count]
       (set (.readObject rdr))))})

(def ^:private write-handlers (-> (merge collection-write-handlers fressian/clojure-write-handlers)
                                  fressian/associative-lookup
                                  fressian/inheritance-lookup))

(def ^:private read-handlers (-> (merge collection-read-handlers fressian/clojure-read-handlers)
                                 fressian/associative-lookup))

(defn encode
  [o]
  (-> o
      (fressian/write :handlers write-handlers)
      .array))

(defn decode
  [buf]
  (let []
    (fressian/read buf :handlers read-handlers)))
