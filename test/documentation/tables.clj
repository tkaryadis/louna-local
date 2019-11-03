(ns documentation.tables
  (:use louna.louna
        louna.louna-util
        queries.q))

;;Read-tables test

(def t (q (:table (:person ?pid ?firstname ?lastname ?city ?country ?continent)
            ;(= ?pid 1005)
            )
          (:do (prn ?qtable))))

#_(q {:q-out ["print"]}
   [?pid ?pid2 ?pid3]
   (:table (:person ?pid ?firstname ?lastname ?city ?country ?continent)
           ((+ ?pid 2) ?pid2)
           ((+ ?pid2 3) ?pid3)
           (> ?pid3 1009))
   #_(:sort-by (desc ?pid))
   #_(:group-by ((apply + ?pid) ?pids1)
              ((apply + ?pid) ?pids2)
              (> ?pids1 10044)
              (> (+ ?pids1 ?pids2) 9000)))

#_(q {:q-out ["print"]}
   (:table t (= ?pid 1005) ((+ ?pid 2) ?pid2)))
