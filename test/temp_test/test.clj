(ns temp-test.test
  (:use louna.louna
        louna.louna-util))

(c {:c-out ["print"]}
   (:rdf "ex104"))

#_(q {:q-in ["ex002"] :q-out ["print"]}

   [?craigEmail]
   (:ab.email :ab.craig ?craigEmail))

#_(q {:q-in ["ex100"] :q-out ["print"]}

   [?first ?last ?instrument]

   (:ab.firstName ?person ?first)
   (:ab.lastName ?last)
   (:ab.instrument ?instrument)
   ((:ab.instrument ?person "sax")
      (:add (:ab.instrument ?person "trumpet"))))

#_(comment
   [c0 c1 c2]
   :n.value22 "value23" "value24"
   :n.value0 "value1" "value2"
   :n.value44 "value45" "value46"
   :n.value33 "value34" "value35"
   :n.value11 "value12" "value13"
   :n.value77 "value78" "value79"
   :n.value55 "value56" "value57"
   :n.value66 "value67" "value68"
   :n.value88 "value89" "value90"
   :n.value99 "value100" "value101")

#_(q {:q-in ["genDB"] :q-out ["print"]}
   (:n.c1 ?c0 ?c1)
   ;(?r ?c0 "value1")

   ;(:n.c1 ?c0 ?c1)
   ;(:n.c2 ?c0 ?c2)
   ;(:n.c3  ?c0 ?c3)
   ;(:n.c4  ?c0 ?c4)
   ;(:n.c5  ?c0 ?c5)
   ;(:n.c6  ?c0 ?c6)
   ;(:n.c7  ?c0 ?c7)
   ;(:n.c8  ?c0 ?c8)
   ;(:n.c9  ?c0 ?c9)
   ;(:n.c10 ?c0 ?c10)
   ;(clojure.string/ends-with? ?c1 "1")
   ;(clojure.string/ends-with? ?c2 "2")
   ;(clojure.string/ends-with? ?c3 "3")
   ;(clojure.string/ends-with? ?c4 "4")
   ;(clojure.string/ends-with? ?c5 "5")
   ;(clojure.string/ends-with? ?c6 "6")
   ;(clojure.string/ends-with? ?c7 "7")
   ;(clojure.string/ends-with? ?c8 "8")
   ;(clojure.string/ends-with? ?c9 "9")
   ;(clojure.string/ends-with? ?c10 "10")
   #_(and (clojure.string/ends-with? ?c1 "1")
         (clojure.string/ends-with? ?c2 "2"))
   ;((str ?c1 ?c2) ?c1c2)
   ;(clojure.string/ends-with? ?c1c2 "2")
   ;((str ?c1c2 ?c3) ?c1c2c3)
   ;(clojure.string/ends-with? ?c1c2c3 "3")

   #_(or (clojure.string/ends-with? ?c3 "3")
         (clojure.string/ends-with? ?c4 "4"))
   ;(:group-by ((count ?c0) ?rows))
   )