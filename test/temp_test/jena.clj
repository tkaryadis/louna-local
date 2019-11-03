(ns temp-test.jena
  (:use louna.louna
        louna.louna-util)
  (:require state.db-settings
            benchmark.jena-run
            benchmark.benchmark-info
            benchmark.sparql-queries))


#_(def sparql-str (str " PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                       PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                       CONSTRUCT { ?x  vcard:N _:v .
                                   _:v vcard:givenName ?gname .
                                   _:v vcard:familyName ?fname }
                       WHERE
                                 {{ ?x foaf:firstname ?gname }
                                  UNION
                                  { ?x foaf:givenname   ?gname } .
                                  { ?x foaf:surname   ?fname }
                                  UNION
                                  { ?x foaf:family_name ?fname } . }"))


;;x gmane
;;:_.node1cpd80gefx2 "Bob"
;;:_.node1cpd80gefx1 "Alice"

;;x fname
;;:_.node1cpd81jhlx2 "Hacker"
;;:_.node1cpd81jhlx1 "Hacker"

;UNION
;{ ?x foaf:familyname ?fname }

(def sparql-str (str " PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
                       PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>
                       SELECT ?x ?gname ?fname
                       WHERE  {
                                 { ?x foaf:firstname ?gname }
                                  UNION
                                 { ?x foaf:givenname   ?gname }
                                 { ?x foaf:surname   ?fname }

                               }"))


(let [- (prn "Jena time :")
      sorted-vars ["x" "gname" "fname"]
      - (time (benchmark.jena-run/run-jena sparql-str
                                           (str (state.db-settings/get-rdf-path) "web10.3.1/10.3.1gen.nt")
                                           sorted-vars))])