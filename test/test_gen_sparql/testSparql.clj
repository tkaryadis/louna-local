(ns test-gen-sparql.testSparql
  (:use macroql.to-sparql)
  (:use macroql.sparql-f))

;;BGP
#_(ql [?person ?craigEmail]
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail))

;;Groups
#_(ql {:q-in ["ex012"] :q-out ["print"]}
   [?person ?craigEmail]
   (:ab.firstName ?person "Craig")
   (:ab.lastName  ?person "Ellis")
   (:ab.email     ?person ?craigEmail)
   (starts-with? ?craigEmail "craigEmail")
   (:if (:ab.firstName ?person "Craig")
        (:ab.lastName  ?person "Ellis")
        (starts-with? ?craigEmail "craigEmail")
        (:ab.email     ?person ?craigEmail)))

;;Nested Groups
#_(ql [?person ?craigEmail]
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail)
    (:if (:ab.firstName ?person "Craig")
         (:ab.lastName  ?person "Ellis")
         (:ab.email     ?person ?craigEmail)
         (:not (:ab.firstName ?person "Craig")
               (:ab.lastName  ?person "Ellis")
               (:ab.email     ?person ?craigEmail)
               (:if (:ab.firstName ?person "Craig")
                    (:ab.lastName  ?person "Ellis")
                    (:ab.email     ?person ?craigEmail))))
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail))

;;filters
#_(ql [?person ?craigEmail]
        (:ab.firstName ?person "Craig")
        (:ab.lastName  ?person "Ellis")
        (:ab.email     ?person ?craigEmail)
        (= ?craigEmail "craigEmail")
        (= ?craigEmail (count "craigEmail"))
        (= (str "po" "craigEmail") "pope")
        (= (str "po" (str "po" "craigEmail")) "pope"))

;;binds+filters+bind(with if inside)
#_(ql [?person ?craigEmail]
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail)
    (starts-with? ?craigEmail "craigEmail")
    ((= (str "po" (str "po" "craigEmail")) "pope") ?b)
    ((iff (< (month ?deathDate) (month ?birthDate))
          1
          0) ?validDate))

;;group-by
#_(ql [distinct ?x ((sum ?x) ?z)]
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail)
    (starts-with? ?craigEmail (str "craig" "Email"))
    (:if (:ab.firstName ?person "Craig"))
    (:group-by ?x ?y
               (< (sum ?x) 10))
    (:sort-by ?x (desc ?y) (asc ?z) (desc (avg (sum ?w))))
    (:limit 100))

#_(ql [distinct ?x ?z]
    (:ab.firstName ?person "Craig")
    (:ab.lastName  ?person "Ellis")
    (:ab.email     ?person ?craigEmail)
    (starts-with? ?craigEmail (str "craig" "Email"))
    (:if (:ab.firstName ?person "Craig"))
    (:group-by ?x ?y
      (< (sum ?x) 10)
      ((sum ?x) ?z))
    (:sort-by ?x (desc ?y) (asc ?z) (desc (avg (sum ?w))))
    (:limit 100))

#_(ql {:q-in ["ex012"] :q-out ["print"]}
      [?k]
      (:ab.firstName ?person "Craig")
      ((str "abcdefg" (* 2 (/ (+ 3 4 6 5) 10)) "cd") ?z)
      (or ?x1 ?x2 ?x3 ?x4 ?x5)
      ((subs ?z 2 4) ?k)
      (includes? ?z ?x))