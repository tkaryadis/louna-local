(ns documentation.delete-files
  (:use louna.louna
        louna.louna-util
        documentation.shared-functions)
  (:require [clojure.java.io :as io]))

;;This example demostrates the use of do/do-each to run functions while the query is running
;;and use the intermidiate results
;; do=> run 1 time,args=collumns,  do-each => run 1 time per value of argument

;;remove all the files in the directory  "/home/white/IdeaProjects/louna/data-test"
;;tha dont ends with extensions #{".nt" ".rq" ".dns"}
;;this can be used to delete all the files that louna produces from the .nt file
;;(like 1 relation/file stats etc)


;;do
#_(q (:ab.files ?file ?path)
   ((get-file-extension ?file) ?ext)                  ;;bind (get-file-extension is simple clojure function defined in shared-functions.clj)
   (not (contains? #{".nt" ".rq" ".dns"} ?ext))       ;;filter,keep only files that DONT end with extension  .nt,.rq,.dns
   (:do (dorun (map io/delete-file ?path)))           ;;delete the files(not in the query,in the filesystem)
   (c ((get-files-paths (get-dbs-path)) :ab.files)))


;;do-each
#_(q (:ab.files ?file ?path)
   (:do (prn "Number of files :" (count ?file)))
   (:do-each (do (println ?file)))
   (c ((get-files-paths (get-dbs-path)) :ab.files)))

#_(q (:ab.files ?file ?path)
   (:do (prn "Hello Louna"))
   (c ((get-files-paths (get-dbs-path)) :ab.files)))

#_(q (:ab.files ?file ?path)
   (:do-each (prn "Hello Louna"))
   (c ((get-files-paths (get-dbs-path)) :ab.files)))

;;do and do-each
#_(q (:ab.files ?file ?path)
   (:do-each (do (prn "Hello the table so far")
            (prn "Lines count = " (count (get ?qtable "table")))
            (louna.louna-util/print-table ?qtable)))
   (c ((get-files-paths (get-dbs-path)) :ab.files)))