(ns documentation.rdf
  (:use louna.louna
        louna.louna-util
        macroql.to-sparql)
  (:require macroc.c-rdf-in))

#_(def rdf-dirs ["ex002"
               "ex012"
               "ex041"
               "ex054"
               "ex069"
               "ex074"
               "ex100"
               "ex104"
               "ex122"
               "ex122-123"
               "ex138"
               "ex145"
               "ex187"
               "ex198"
               "sp1"
               "spValues1"])

#_(dorun (map (fn [rdf-dir]
              (c {:c-out [rdf-dir]}
                 (:rdf rdf-dir)))
            rdf-dirs))

#_(c {:c-out ["print" "ttls"]}
   (:rdf "ttls"))


(q {:q-in ["ttls"] :q-out ["print"]}
   [?foafName ?mbox ?gname ?fname]

   (:foaf.name ?x ?foafName)
   (:if (:foaf.mbox ?x ?mbox))
   (:if (:vcard.N ?x ?vc)
        (:vcard.Given ?vc ?gname)
        (:if (:vcard.Family ?vc ?fname))))