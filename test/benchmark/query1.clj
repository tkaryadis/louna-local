(ns benchmark.query1
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))

;;About this query benchmark
;;About the benchmark data read benchmark_info.clj

;;Systems compared
;;1)Louna
;;2)Jena

;;They will both read the same RDF data
;;Data will be loaded in memory
;;Louna will convert them in memory to a hash-table of relations

;;   Query1=just make the joins to join the DB back to the original table(10 joins)
;;   To minimize printing results we use an aggregate function in the end

;;Louna here is doing 2 things
;;1)converting rdf data to louna-data
;;2)query them

;;1 row of initial table = 9 triples
;;1000 rows
;;  louna    1.3 sec
;;  jena     2   sec
;;10000 rows
;;  louna    7.1 sec
;;  jena     3.6 sec
;;100000rows
;;  louna    72 sec
;;  jena     19 sec
;;200000rows
;;  louna    140 sec
;;  jena      - (out of memory : GC overhead limit exceeded)
;;1x-4x for louna to query RDF data in .nt format

(def rows 100000)  ;;you only change this

(set-join-method "disk")

(let [- (library.util/delete-dir-recursively (str (get-dbs-path) "genDB"))
      - (library.util/delete-dir-recursively (str (get-rdf-path) "genDB"))
      genTable (benchmark.benchmark-info/gen-table 10 rows)
      - (benchmark.benchmark-info/save-db genTable "genDB")
      - (spit (java.io.File. (str (get-dbs-path) "genDB/genDB.ns")) {"n" "http://louna/ns/"})
      - (db-to-nt "genDB")])


(defn louna-bench []
  (q {:q-out ["print"]}
     (:n.c1  ?c0 ?c1)
     (:n.c2  ?c0 ?c2)
     (:n.c3  ?c0 ?c3)
     (:n.c4  ?c0 ?c4)
     (:n.c5  ?c0 ?c5)
     (:n.c6  ?c0 ?c6)
     (:n.c7  ?c0 ?c7)
     (:n.c8  ?c0 ?c8)
     (:n.c9  ?c0 ?c9)
     (:group-by ((count ?c0) ?rows))
     (c (:rdf "genDB"))))

(println "Louna time :")
(time (louna-bench))

(def sparql-str (str " PREFIX n: <http://louna/ns/> "
                     (benchmark.sparql-queries/get-sparql-query 1)))

(let [- (prn "Jena time :")
      sorted-vars ["rows"]
      - (time (benchmark.jena-run/run-jena sparql-str
                                           (str (state.db-settings/get-rdf-path) "genDB/genDB.nt")
                                           sorted-vars))])