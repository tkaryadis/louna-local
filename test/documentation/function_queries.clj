(ns documentation.function-queries
  (:use louna.louna
        louna.louna-util
        documentation.shared-functions)
  (:require [clojure.java.io :as io]))


;;--------------------------------------Function queries---------------------------------------------------


;;Queries general with arguments.Modify behaviour based on the argument of the function

#_(defn delete-except-extensions [path extensions]
   (q (:ab.files ?file ?path)
      (:do (prn "All files :" (count ?file)))
      ((let [ext (get-file-extension ?file)]
          ext) ?ext)
      (not (contains? extensions ?ext))
      (:do (do (println "Files that will be deleted :" (count ?file))
               (dorun (map io/delete-file ?path))))
      (:do-each (do (println "Deleted : " ?file)))
      (c ((get-files-paths path) :ab.files))))


#_(delete-except-extensions (get-dbs-path) #{".nt" ".rq" ".dns"})


;;;;---------------------------------Function construncts/function queries------------------------------------

;;original query from the book
#_(c {:q-in ["ex187"] :c-out ["print"]}
   (:ab.hasGrandfather ?child ?gparent)
   (q (:ab.hasParent ?child    ?parent)
      (:ab.hasParent ?parent   ?gparent)
      (:ab.gender    ?gparent  :d.male)))

;;ancestor 2 levels
#_(defn add-ancestor-2 [ancestor gender]
  (c {:q-in ["ex187"] :c-out ["print"]}
     (ancestor ?child ?gparent)
     (q (:ab.hasParent ?child    ?parent)
        (:ab.hasParent ?parent   ?gparent)
        (:ab.gender    ?gparent  gender))))

#_(add-ancestor-2 :ab.hasGrandfather :d.male)

;;results are empty,no-one has a grandmother in the data
#_(add-ancestor-2 :ab.hasGrandmother :d.female)


;;--------------------------------3 defining rules------------------------------------------

#_(defn grand-fathers []
  (q {:q-in ["ex187"] :q-out ["print"]}
     (:ab.hasParent ?child    ?parent)
     (:ab.hasParent ?parent   ?gparent)
     (:ab.gender    ?gparent  :d.male)))

#_(grand-fathers)


(defn has-grandparent? [child gparent]
  (? (q {:q-in ["ex187"]}
        (:ab.hasParent child     ?parent)
        (:ab.hasParent ?parent   gparent))))

(prn (has-grandparent? :d.jane :d.pat))