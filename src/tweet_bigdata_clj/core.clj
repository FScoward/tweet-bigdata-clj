(ns tweet-bigdata-clj.core
  (:import [twitter4j TwitterFactory Query QueryResult])
  (:require [clojure-csv.core :as csv]
            [clojure.java.io :as io]))

(def twitter (.getInstance (TwitterFactory.)))

(defn get-tweets [hash-tag]
  "tweet取得"
  (let [query (doto (Query.)
                (.setQuery hash-tag)
                (.setCount 100)
                (.setSince "2014-11-03")
                (.setUntil "2014-11-04"))]
    (->> (.search twitter query)
         (.getTweets))))

(defn tweets-to-map [tweets]
  "tweetsをmap形式に変換する"
  (map #(zipmap [:screenName
                 :name
                 :text
                 :createdAt]
                [(.. % getUser getScreenName)
                 (.. % getUser getName)
                 (.getText %)
                 (.. % getCreatedAt toString)]) tweets))

(defn to-csvfile [text]
  "csvファイルを作成"
  (with-open [out-file (io/writer "out-file.csv" :encoding "sjis")]
    (.write out-file (apply str text))))

(defn to-csv [{text :text name :name screen-name :screenName created-at :createdAt}]
  "カンマ分割"
  (csv/write-csv [[name screen-name text created-at]]))

; sinceId でQueryを返してループ回す
(defn get-tweets-all [#^QueryResult query]
  (loop [q query]
    (when (.hasNext q)
      (Thread/sleep 5000)
      (let [nq (.nextQuery q)
            nqr (.search twitter nq)]
        (recur nqr))
      )))

;; repl ----------------------
(def tweets (get-tweets "#zanmai"))
(def map-val (tweets-to-map tweets))
;(def x (first map-val))

;(def text (->> map-val (map #(to-csv %))))
;(to-csvfile)

(def q (doto (Query.)
         (.setQuery "#zanmai")
         (.setCount 100)
         (.setUntil "2014-11-04")
         (.setSince "2014-11-03")))

(def t (.search twitter q))
(.hasNext t)
(.nextQuery t)
;(get-tweets-since t)
(lazy-seq get-tweets-all t)
;(.hasNext t)
;(.nextQuery t)
;(.getMaxId t)
;(.getSinceId t)
;(.sinceId q 529193567361564674)

;;---------------------------



(defn -main []
  (let [tweets (get-tweets "#zanmai")
        map-val (tweets-to-map tweets)
        text (->> map-val (map #(to-csv %)))]
    (to-csvfile text)))
