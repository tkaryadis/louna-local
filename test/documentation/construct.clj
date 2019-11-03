(ns documentation.construct
  (:use louna.louna
        louna.louna-util))


#_(def parentsDB {
                :ab.hasParent
                [[:d.jane :d.gene]
                 [:d.gene :d.pat]
                 [:d.joan :d.pat]
                 [:d.mike :d.joan]]

                :ab.gender
                [[:d.gene :d.female]
                 [:d.joan :d.female]
                 [:d.pat :d.male]]
                })

#_(def q1Table (q {:q-in [parentsDB]}
                (:ab.hasParent  ?person ?parent)
                (:ab.hasParent  ?parent ?gparent)
                (:ab.hasParent  ?aunt   ?gparent)
                (:ab.gender             :d.female)
                (not= ?parent ?aunt)))


#_(c {:c-in [q1Table] :c-out ["print"]}
   (:ab.hasAunt ?person ?aunt))


#_(c {:q-in [parentsDB] :c-out ["print"]}
   (:ab.hasAunt ?person ?aunt)
   (q (:ab.hasParent  ?person ?parent)
      (:ab.hasParent  ?parent ?gparent)
      (:ab.hasParent  ?aunt   ?gparent)
      (:ab.gender             :d.female)
      (not= ?parent ?aunt)))


#_(c {:c-out ["print"]}
   (:ab.queriesFiles ?y ?x)
   (q (:ab.files ?x ?y)
      (clojure.string/ends-with? ?x ".rq")
      (c ((get-files-paths (get-dbs-path)) :ab.files))))




