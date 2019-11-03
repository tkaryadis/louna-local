(ns web-qc.c
  (:use louna.louna
        louna.louna-util))

;;query 10.3 Constructing an Output Graph
#_(c {:c-out ["print"]}
     (:vcard.FN :person.alice ?name)
     (q {:q-out ["print"]}
        (:foaf.name ?x ?name)
        (c (:rdf "web10.3"))))

#_(c {:c-out ["print"]}
   (:rdf "web10.3.1"))

;;PRIORITIES !
;;query 10.3.1 Templates with Blank Nodes, blank nodeS?
#_(c {:c-out ["print"]}
   (:vcard.N ?x :blank.v)
   (:vcard.givenName :blank.v ?gname)
   (:vcard.familyName :blank.v ?fname)
   (q {:q-out ["print"]}
      ((:foaf.firstname ?x ?gname)
       (:add (:foaf.givenname ?x ?gname)))
      ((:foaf.surname ?x ?fname)
       (:add (:foaf.familyname ?x ?fname)))
      (c (:rdf "web10.3.1"))))






