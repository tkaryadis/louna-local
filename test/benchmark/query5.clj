(ns benchmark.query5
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))

;;1000 rows
;;  louna    0.37 sec
;;  jenaTDB  0.75 sec
;;10000 rows
;;  louna    2.4 sec
;;  jenaTDB  2.1 sec
;;100000rows
;;  louna    20 sec
;;  jenaTDB  9 sec
;;200000rows
;;  louna    44 sec
;;  jenaTDB  12 sec
;;400000rows
;;  louna    103 sec
;;  jenaTDB  26 sec
;;2x-4x

(def rows 200000) ;;you only change this

(set-join-method "disk")

(library.util/delete-dir-recursively (str (get-dbs-path) "genDB"))
(library.util/delete-dir-recursively (str (get-dbs-path) "tdb"))

(let [genTable (benchmark.benchmark-info/gen-table 11 rows)
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
     (:sort-by (str ?c1 ?c2) ?c3 (str ?c4 ?c1) (desc ?c4) (asc (str ?c5 ?c3)))
     (:limit 1)))

(println "Louna time :")
(time (run-bench))


(def sparql-str (str " PREFIX n: <http://louna/ns/> "
                     (benchmark.sparql-queries/get-sparql-query 5)))

(db-to-nt "genDB")

(let [- (benchmark.jena-run/store-rdf (str (state.db-settings/get-rdf-path)  "genDB/genDB.nt"))
      - (println "Jena TDB time :")
      sorted-vars ["c0" "c1" "c2" "c3" "c4" "c5" "c6" "c7" "c8" "c9"]]
  (time (benchmark.jena-run/run-jena-tdb sparql-str sorted-vars)))