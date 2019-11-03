(ns benchmark.query2
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))

;;About this query benchmark
;;About the benchmark data read benchmark_info.clj

;;Louna (reading data from files)
;;Jena TDB (reading data from files)
;;(Jena that loads RDF in memory is slow and needs much memory
;; i don't show the results ,as shown in query1 it goes up to 100.000 rows with time 16 sec)

;;OLD
;;1 row=10 triples
;;1000 rows
;;  louna    460ms
;;  jenaTDB  830ms
;;10000 rows
;;  louna    1.7 sec
;;  jenaTDB  1.8 sec
;;100000rows
;;  louna    11.3 sec
;;  jenaTDB  5 sec
;;200000rows
;;  louna    24 sec
;;  jenaTDB  12 sec
;;300000rows
;;  louna    35 sec
;;  jenaTDB  17 sec
;;400000rows
;;  louna    50 sec
;;  jenaTDB  25 sec
;;1x-2x

(def rows 100000)  ;;you only change this

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
     (:group-by ((count ?c0) ?rows))))


(println "Louna time :")
(time (run-bench))


(def sparql-str (str " PREFIX n: <http://louna/ns/> "
                     (benchmark.sparql-queries/get-sparql-query 1)))

(db-to-nt "genDB")

(let [- (println "Preparing jenaTDB to run ...")
      - (benchmark.jena-run/store-rdf (str (state.db-settings/get-rdf-path) "genDB/genDB.nt"))
      - (println "Jena TDB time :")
      sorted-vars ["rows"]]
  (time (benchmark.jena-run/run-jena-tdb sparql-str sorted-vars)))