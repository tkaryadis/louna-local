(ns benchmark.sparql-queries
  (:use macroql.to-sparql)
  (:use macroql.sparql-f)
  (:require louna.louna-util))


(def queries
  {1
   (ql {:q-in ["genDB"] :q-out ["print"]}
       (:n.c1  ?c0 ?c1)
       (:n.c2  ?c0 ?c2)
       (:n.c3  ?c0 ?c3)
       (:n.c4  ?c0 ?c4)
       (:n.c5  ?c0 ?c5)
       (:n.c6  ?c0 ?c6)
       (:n.c7  ?c0 ?c7)
       (:n.c8  ?c0 ?c8)
       (:n.c9  ?c0 ?c9)
       (:group-by ((count ?c0) ?rows)))

   3
   (ql {:q-in ["genDB"] :q-out ["print"]}
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
       (ends-with? ?c1 "1")
       (ends-with? ?c2 "2")
       (ends-with? ?c3 "3")
       (ends-with? ?c4 "4")
       (ends-with? ?c5 "5")
       (ends-with? ?c6 "6")
       (ends-with? ?c7 "7")
       (ends-with? ?c8 "8")
       (ends-with? ?c9 "9")
       ;;table filters (applied at join time)
       (or  (ends-with? ?c1 "2")
            (ends-with? ?c2 "2"))
       (or  (ends-with? ?c3 "4")
            (ends-with? ?c4 "4")
            (ends-with? ?c5 "5"))
       (or  (ends-with? ?c6 "4")
            (ends-with? ?c7 "4")
            (ends-with? ?c8 "4")
            (ends-with? ?c9 "9"))

       ;;P artially True filters
       (or (starts-with? (subs ?c1 5 (strlen ?c1)) "1")
           (starts-with? (subs ?c1 5 (strlen ?c1)) "2")
           (starts-with? (subs ?c1 5 (strlen ?c1)) "3")
           (starts-with? (subs ?c1 5 (strlen ?c1)) "4")
           (starts-with? (subs ?c1 5 (strlen ?c1)) "5"))
      (:group-by ((count ?c0) ?rows)))

   4
   (ql {:q-in ["genDB"] :q-out ["print"]}
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

       (ends-with? ?x1c2 "2")
       (ends-with? ?y1c2 "2")

       (ends-with? ?x2c3 "3")
       (ends-with? ?y2c3 "3")

       (ends-with? ?x3c4 "4")
       (ends-with? ?y3c4 "4")

       (ends-with? ?x4c5 "5")
       (ends-with? ?x4c5 "5")

       (ends-with? ?y1c2 "2")
       (ends-with? ?y2c3 "3")

       (ends-with? ?y3c4 "4")
       (ends-with? ?y4c5 "5")

       (or (starts-with? (subs ?y2c3 5 (strlen ?y2c3)) "1")
           (starts-with? (subs ?y2c3 5 (strlen ?y2c3)) "2")
           (starts-with? (subs ?y2c3 5 (strlen ?y2c3)) "3")
           (starts-with? (subs ?y2c3 5 (strlen ?y2c3)) "4")
           (starts-with? (subs ?y2c3 5 (strlen ?y2c3)) "5"))


       (:group-by ((count ?c0) ?rows))
       )

   5
   (ql {:q-in ["genDB"] :q-out ["print"]}
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
      (:limit 1))
   }
  )

;;-----------query1------------------------

(defn get-sparql-query [qn]
  (let [q (get queries qn)
        ;- (println q)
        ]
    (louna.louna-util/ql-str q)))

