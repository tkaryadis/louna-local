(ns book-qc.paths
  (:use louna.louna
        louna.louna-util))

;;--------------------------property paths----------------------------------------

(set-join-method "disk")

[1]
;;ex083.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (%:c.cites :a.paperA ?s))

[2]
;;sparql_query = ex077.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites ?s :a.paperA))


[3]
;;ex075
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s ?title]
      (:dc11.title|:rdfs.label ?s ?title))


[4]
;;ex082
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      ;[?s]
      (:c.cites-:c.cites-:c.cites ?s :a.paperA))

[5]
;;ex084.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites-%:c.cites ?s :a.paperF)
      (not= ?s :a.paperF))

[6]
;;ex078.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites+ ?s :a.paperA))


[7]
;;my-test
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites+ :a.paperD ?s)
      (contains? #{:a.paperB} ?s))

[8]
;;my-test
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s ?o]
      (:c.cites+ ?s ?o)
      (and (contains? #{:a.paperD} ?s) (contains? #{:a.paperB} ?o)))

[9]
;;q1.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites*-:dc11.title ?s "Paper A")
      (:sort-by ?s))

[10]
;;q2.rq
#{}(q {:q-in ["ex074"] :q-out ["print"]}

      [?s]
      (:c.cites+-:dc11.title ?s "Paper A")
      (:sort-by ?s))