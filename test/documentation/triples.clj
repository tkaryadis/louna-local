(ns documentation.triples
  (:use louna.louna
        louna.louna-util))

#_(def personDB {
               :person.firstName
               [[:person.i0432 "Richard"]
                [:person.i9771 "Cindy"]
                [:person.i8301 "Craig"]]

               :person.lastName
               [[:person.i0432 "Mutt"]
                [:person.i9771 "Marshall"]
                [:person.i8301 "Ellis"]]

               :person.email
               [[:person.i0432 "richard49@hotmail.com"]
                [:person.i9771 "cindym@gmail.com"]
                [:person.i8301 "c.ellis@usairwaysgroup.com"]]
               })

#_(q {:q-in [personDB] :q-out ["print"]}
   (?r ?s ?o))

#_(q {:q-in [personDB] :q-out ["print"]}
   (?r ?s ?o)
   (or (= ?r :person.lastName) (= ?r :person.firstName)))