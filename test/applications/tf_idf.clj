(ns applications.tf-idf
  (:use louna.louna
        louna.louna-util)
  (:require [clojure.string :as s]))

(def db {:text [[1 "Aristotle and Plato"]
                [2 "the athenian democracy"]
                [3 "democrats versus republicans"]
                [4 "democracy versus republic"]
                [5 "Socrates Plato and Aristotle"]
                [6 "democrary versus mediocracy"]
                [7 "Superman versus Batman and Catwoman"]]})

(def ndocs (count (get db :text)))

(def docTerms
  (q {:q-in [db]}
     [?docid ?docTerms ?docSet]
     (:text ?docid ?doc)
     ((s/split ?doc #"\s+") ?docVec)
     ((map (comp s/lower-case s/trim) ?docVec) ?docTerms)
     ((into #{} ?docTerms) ?docSet)))

(defn get-df [term]
  (let [qr (q (:table docTerms)
              ((if (contains? ?docSet term) 1 0) ?contains)
              (:group-by ((apply + ?contains) ?df)))]
    (first (get-first-line qr))))

(def get-df-mem (memoize get-df))

(defn log2 [x]
  (/ (Math/log x) (Math/log 2)))

(defn get-tfidf [frequencies maxfreq]
  (reduce (fn [freq-tf [key freq]]
            (let [tf (/ freq maxfreq)
                  df (get-df-mem key)
                  idf (log2 (/ ndocs df))
                  w (* tf idf)]
              (assoc freq-tf key w)))
          {}
          frequencies))

(q {:q-out ["print"]}
   [?docid ?tfidf]
   (:table docTerms)
   ((frequencies ?docTerms) ?frequencies)
   ((apply max (vals ?frequencies)) ?maxfreq)
   ((get-tfidf ?frequencies ?maxfreq) ?tfidf))