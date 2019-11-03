(ns web-qc.q
  (:use louna.louna
        louna.louna-util))

;;examples from https://www.w3.org/2001/sw/DataAccess/rq23/examples.html
;;Some differences
;;1)Louna inside stores URI'S as  :prefix.name,if the user has give the prefix-URI pair
;;  for example in a .ttl file,it will use the user's prefix,if not it will auto-generate a prefix
;;  for exampl p1,p2 ....
;;  We never use URI's in louna queries.We use the notation  :prefix.name
;;  To see the mathings we see at ns file that louna generates,but we generally dont have to look at it
;;  except for prefixes that we havent used in the .ttl file and its louna's auto-generated
;;2)Louna will convert the empty namespace to  :empty.name for example :book1 = :empty.book1 inside louna
;;3)language tag is ignored by louna "cat"@en for louna is just "cat"
;;4)special datatype for example "abc"^^dt:specialDatatype  for louna its only "abc"
;;5)dates are like strings
;;6)URI metadata are ignores for example <mailto:test@gmail.com> is "test@gmail.com" for Louna


;;query 2.4
#_(q {:q-out ["print"]}
   [?book ?title]
   (:dc.title ?book ?title)
   (c (:rdf "web2.4")))


;;query 2.5.3
#_(q {:q-out ["print"]}
   [?mbox]
   (:foaf.name ?x "Johnny Lee Outlaw")
   (:foaf.mbox    ?mbox)
   (c (:rdf "web2.5.3")))


;;query 2.6
#_(q {:q-out ["print"]}
   [?name ?mbox]
   (:foaf.name ?x ?name)
   (:foaf.mbox ?x ?mbox)
   (c (:rdf "web2.6")))

;;query 2.7
#_(q {:q-out ["print"]}
   [?x ?name]
   (:foaf.name ?x ?name)
   (c (:rdf "web2.7")))

;;2.9
#_(q {:q-out ["print"]}
   [?book ?title]
   (:dc.title ?book ?title)
   (c (:rdf "web2.9")))

;;2.9 q1
#_(q {:q-out ["print"]}
   [?book ?title]
   (:rdf.subject    ?t ?book)
   (:rdf.predicate  ?t :dc.title)
   (:rdf.object     ?t ?title)
   (:empty.saidBy   ?t "Bob")
   (c {:c-out ["print"]}
      (:rdf "web2.9")))

;;3.1
#_(q {:q-out ["print"]}
   [?v]
   (?p ?v 42)
   (c (:rdf "web3.1")))

;;3.1
#_(q {:q-out ["print"]}
   [?x]
   (?p ?x "cat")
   (c (:rdf "web3.1")))

;;3.2
#_(q {:q-out ["print"]}
   [?title ?price]
   (:ns.price ?x ?price)
   (< ?price 30)
   (:dc.title ?x ?title)
   (c (:rdf "web3.2")))

;;4.1(its on web2.6 data)
#_(q {:q-out ["print"]}
   [?name ?mbox]
   (:foaf.name ?x ?name)
   (:foaf.mbox ?x ?mbox)
   (c (:rdf "web2.6")))

;;query 4.1-q1
#_(q {:q-out ["print"]}
   [?name ?mbox]
   ((:foaf.name ?x ?name))
   ((:foaf.mbox ?x ?mbox))
   (c {:c-out ["print"]}
      (:rdf "web2.6")))

;;query 5.1
#_(q {:q-out ["print"]}
   [?name ?mbox]
   (:foaf.name ?x ?name)
   (:if (:foaf.mbox ?x ?mbox))
   (c (:rdf "web5.1")))

;;query 5.2
#_(q {:q-out ["print"]}
   [?title ?price]
   (:dc.title ?x ?title)
   (:if (:ns.price ?x ?price)
        (< ?price 30))
   (c (:rdf "web5.2")))

;;query 5.3
#_(q {:q-out ["print"]}
   [?name ?mbox ?hpage]
   (:foaf.name ?x ?name)
   (:if (:foaf.mbox ?x ?mbox))
   (:if (:foaf.homepage ?x ?hpage))
   (c (:rdf "web5.3")))


;;query 5.5
#_(q {:q-out ["print"]}
     [?foafName ?mbox ?gname ?fname]
     (:foaf.name ?x ?foafName)
     (:if (:foaf.mbox ?x ?mbox))
     (:if (:vcard.N ?x ?vc)
          (:vcard.Given ?vc ?gname)
          (:if (:vcard.Family ?vc ?fname)))
     (c (:rdf "web5.5")))

;;query 6.1
#_(q {:q-out ["print"]}
   [?title]
   (:dc10.title ?book ?title)
   (:add (:dc11.title ?book ?title))
   (c (:rdf "web6.1")))

;;query 6.1-q1
#_(q {:q-out ["print"]}
   [?x ?y]
   (:dc10.title ?book ?x)
   (:add (:dc11.title ?book ?y))
   (c (:rdf "web6.1")))

;;query 6.1-q2
;;typo in the webpage,look at data
;;alice wrote "SPARQL Query Language Tutorial"
#_(q {:q-out ["print"]}
   [?title ?author]
   (:dc11.title ?book ?title)
   (:dc11.creator ?book ?author)
   (:add (:dc10.title ?book ?title)
         (:dc10.creator ?book ?author))
   (c (:rdf "web6.1")))


;;7-8-9 are using named graphs ,that louna don't support


;;query 10.1.2 DISTINCT
#_(q {:q-out ["print"]}
   [distinct ?name]
   (:foaf.name ?x ?name)
   (c (:rdf "web10")))

;;query 10.1.3 ORDER BY
#_(q {:q-out ["print"]}
     [?name]
     (:foaf.name ?x ?name)
     (:sort-by ?name)
     (c (:rdf "web10")))

;;query 10.1.3-q1,query 10.1.3-q2 (skipped,they dont give the data)

;;query 10.1.4 LIMIT
#_(q {:q-out ["print"]}
   [?name]
   (:foaf.name ?x ?name)
   (:limit 1)
   (c (:rdf "web10")))

;;query 10.1.5 OFFSET (not implemented in louna but its easy to implemement)

;;query 10.2 Selecting Variables
#_(q {:q-out ["print"]}
   [?nameX ?nameY ?nickY]
   (:foaf.knows ?x ?y)
   (:foaf.name  ?x ?nameX)
   (:foaf.name  ?y ?nameY)
   (:if (:foaf.nick ?y ?nickY))
   (c (:rdf "web10.2")))

;;query 10.5 Asking "yes or no" questions
#_(println (? (q (:foaf.name ?x "Alice")
               (c (:rdf "web10.5")))))

#_(println (? (q (:foaf.name ?x "Alice")
               (:foaf.mbox ?x  "alice@work.example")
               (c (:rdf "web10.5")))))

;;query 11.4.1 bound
#_(q {:q-out ["print"]}
   [?givenName]
   (:foaf.givenName ?x ?givenName)
   (:if (:dc.date ?x ?date))
   (not (nil? ?date))
   (c (:rdf "web11.4")))

;;query 11.4.1-q1
#_(q {:q-out ["print"]}
   [?givenName]
   (:foaf.givenName ?x ?givenName)
   (:if (:dc.date ?x ?date))
   (nil? ?date)
   (c (:rdf "web11.4")))

;;louna dont support metadata URIs like
;;<mail:to..>
;;so the next queries a bit different

;;query 11.4.2
#_(q {:q-out ["print"]}
   [?name ?mbox]
   (:foaf.name ?x ?name)
   (:foaf.mbox ?x ?mbox)
   (not (prefixed? ?mbox))
   (c (:rdf "web11.4.2")))

;;query 11.4.3 isBlank
#_(q {:q-out ["print"]}
   [?given ?family]
   (:a.annotates ?annot :sp.rdf-sparql-query)
   (:dc.creator  ?annot ?c)
   (:if (:foaf.given ?c ?given)
        (:foaf.family   ?family))
   (blank? ?c)
   (c (:rdf "web11.4.3")))

;;query 11.4.5
#_(q {:q-out ["print"]}
     [?name ?mbox]
     (:foaf.name ?x ?name)
     (:foaf.mbox ?x ?mbox)
     (clojure.string/includes? ?mbox "@work.example")
     (c (:rdf "web11.4.5")))

;;query 11.4.7
#_(q {:q-out ["print"]}
   [?name ?shoeSize]
   (:foaf.name ?x ?name)
   (:eg.shoeSize ?x ?shoeSize)
   (integer? ?shoeSize)
   (c (:rdf "web11.4.7")))

;;query 11.4.10
#_(q {:q-out ["print"]}
   [?name1 ?name2]
   (:foaf.name ?x ?name1)
   (:foaf.mbox ?x ?mbox1)
   (:foaf.name ?y ?name2)
   (:foaf.mbox ?y ?mbox2)
   (and (= ?mbox1 ?mbox2)
        (not= ?name1 ?name2))
   (c (:rdf "web11.4.10")))

;;query 11.4.12
#_(q {:q-out ["print"]}
     [?name]
     (:foaf.name ?x ?name)
     (re-matches #"[^a]li.+" ?name)
     (c (:rdf "web11.4.12")))