(ns benchmark.query4
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))

;;Everything like query3,but now we use binds+binds on binds+filters on them

;;1 row=9 triples
;;1000 rows
;;  louna    0.6 sec
;;  jenaTDB  1.2 sec
;;10000 rows
;;  louna    3.3 sec
;;  jenaTDB  3.4 sec
;;100000rows
;;  louna    21 sec
;;  jenaTDB  18 sec
;;200000rows
;;  louna    45 sec
;;  jenaTDB  36 sec
;;Can't go more table has 8 binds
;;1x-1.5x

(def rows 1000) ;;you only change this

(set-join-method "disk")

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

     ((str ?c1 ?c2) ?x1c2)
     ((str ?c1 ?c2 ?c3) ?x2c3)
     ((str ?c1 ?c2 ?c3 ?c4) ?x3c4)
     ((str ?c1 ?c2 ?c3 ?c4 ?c5) ?x4c5)

     ((str ?c3 ?c4 ?x1c2) ?y1c2)
     ((str ?c3 ?c4 ?c5 ?x2c3) ?y2c3)
     ((str ?c3 ?c4 ?c5 ?c6 ?x3c4) ?y3c4)
     ((str ?c3 ?c4 ?c5 ?c6 ?c7 ?x4c5) ?y4c5)

     (clojure.string/ends-with? ?x1c2 "2")
     (clojure.string/ends-with? ?y1c2 "2")

     (clojure.string/ends-with? ?x2c3 "3")
     (clojure.string/ends-with? ?y2c3 "3")

     (clojure.string/ends-with? ?x3c4 "4")
     (clojure.string/ends-with? ?y3c4 "4")

     (clojure.string/ends-with? ?x4c5 "5")
     (clojure.string/ends-with? ?x4c5 "5")

     (clojure.string/ends-with? ?y1c2 "2")
     (clojure.string/ends-with? ?y2c3 "3")

     (clojure.string/ends-with? ?y3c4 "4")
     (clojure.string/ends-with? ?y4c5 "5")

     (or (clojure.string/starts-with? (subs ?y2c3 5) "1")
         (clojure.string/starts-with? (subs ?y2c3 5) "2")
         (clojure.string/starts-with? (subs ?y2c3 5) "3")
         (clojure.string/starts-with? (subs ?y2c3 5) "4")
         (clojure.string/starts-with? (subs ?y2c3 5) "5"))

     (:group-by ((count ?c0) ?rows))

     ))

(println "Louna time :")
(time (run-bench))

(db-to-nt "genDB")

(def sparql-str (str " PREFIX n: <http://louna/ns/> "
                     (benchmark.sparql-queries/get-sparql-query 4)))


(let [- (benchmark.jena-run/store-rdf (str (state.db-settings/get-rdf-path) "genDB/genDB.nt"))
      - (println "Jena TDB time :")
      sorted-vars ["rows"]]
  (time (benchmark.jena-run/run-jena-tdb sparql-str sorted-vars)))