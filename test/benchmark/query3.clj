(ns benchmark.query3
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))

;;Everything like query1,but now we use filters

;;1 row=9 triples
;;1000 rows
;;  louna    0.5 sec
;;  jenaTDB  1   sec
;;10000 rows
;;  louna    2.3 sec
;;  jenaTDB  2.8 sec
;;100000rows
;;  louna    18   sec
;;  jenaTDB  12.5 sec
;;200000rows
;;  louna    33 sec
;;  jenaTDB  28 sec
;;400000rows
;;  louna    75 sec
;;  jenaTDB  65 sec
;;1x

(def rows 100000)  ;;you only change this

(set-join-method "mem")

(library.util/delete-dir-recursively (str (get-dbs-path) "genDB"))
(library.util/delete-dir-recursively (str (get-dbs-path) "tdb"))

(let [genTable (benchmark.benchmark-info/gen-table 10 rows)
      - (benchmark.benchmark-info/save-db genTable "genDB")])

(defn run-bench []
  (q {:q-in ["genDB"] :q-out ["print"]}
     (:n.c1  ?c0 ?c1)
     (:n.c2  ?c0 ?c2)
     (:n.c3  ?c0 ?c3)
     (:n.c4  ?c0 ?c4)
     (:n.c5  ?c0 ?c5)
     (:n.c6  ?c0 ?c6)
     (:n.c7  ?c0 ?c7)
     (:n.c8  ?c0 ?c8)
     (:n.c9  ?c0 ?c9)

     ;;True filters
     ;;relation filters(applied at reading relation)
     (clojure.string/ends-with? ?c1 "1")
     (clojure.string/ends-with? ?c2 "2")
     (clojure.string/ends-with? ?c3 "3")
     (clojure.string/ends-with? ?c4 "4")
     (clojure.string/ends-with? ?c5 "5")
     (clojure.string/ends-with? ?c6 "6")
     (clojure.string/ends-with? ?c7 "7")
     (clojure.string/ends-with? ?c8 "8")
     (clojure.string/ends-with? ?c9 "9")
     ;;table filters (applied at join time)
     (or  (clojure.string/ends-with? ?c1 "2")
          (clojure.string/ends-with? ?c2 "2"))
     (or  (clojure.string/ends-with? ?c3 "4")
          (clojure.string/ends-with? ?c4 "4")
          (clojure.string/ends-with? ?c5 "5"))
     (or  (clojure.string/ends-with? ?c6 "4")
          (clojure.string/ends-with? ?c7 "4")
          (clojure.string/ends-with? ?c8 "4")
          (clojure.string/ends-with? ?c9 "9"))

     ;;Partially True filters(keep 50% of results)
     (or (clojure.string/starts-with? (subs ?c1 5) "1")
         (clojure.string/starts-with? (subs ?c1 5) "2")
         (clojure.string/starts-with? (subs ?c1 5) "3")
         (clojure.string/starts-with? (subs ?c1 5) "4")
         (clojure.string/starts-with? (subs ?c1 5) "5"))
     (:group-by ((count ?c0) ?rows))))

(println "Louna time :")
(time (run-bench))

(db-to-nt "genDB")

(def sparql-str (str " PREFIX n: <http://louna/ns/> "
                     " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
                     (benchmark.sparql-queries/get-sparql-query 3)))


(let [- (benchmark.jena-run/store-rdf (str (state.db-settings/get-rdf-path) "genDB/genDB.nt"))
      - (println "Jena TDB time :")
      sorted-vars ["rows"]]
  (time (benchmark.jena-run/run-jena-tdb sparql-str sorted-vars)))