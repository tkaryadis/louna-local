(ns documentation.address
  (:use louna.louna
        louna.louna-util))

#_(def relations {
                :person.firstName
                [[1 "Eleni"]
                 [2 "Kostas"]]

                :person.lastName
                [[1 "Papadopoulou"]
                 [2 "Georgiou"]]

                :person.address
                [[1 1]]

                :address.postalCode
                [[1 "17562"]]

                :address.city
                [[1 "ATHENS"]]

                :address.streetAddress
                [[1 "Ahilleos"]]

                :address.region
                [[1 "Attika"]]
                })

;;Both of them have firstname+lastname
#_(q {:q-in [relations] :q-out ["print"]}

   (:person.firstName   ?person  ?firstName)
   (:person.lastName             ?lastName))

;;[person firstName lastName]
;;1 "Eleni" "Papadopoulou"
;;2 "Kostas" "Georgiou"

;;But address has only eleni
#_(q {:q-in [relations] :q-out ["print"]}

   (:person.firstName   ?person  ?firstName)
   (:person.lastName             ?lastName)
   (:person.address              ?address))

;;[person address firstName lastName]
;;1 1 "Eleni" "Papadopoulou"

;;Lets see the complete data
#_(q {:q-in [relations] :q-out ["print"]}

   [?firstName ?lastName ?postalCode ?city ?streetAddress ?region]
   (:person.firstName   ?person  ?firstName)
   (:person.lastName             ?lastName)
   (:person.address              ?address)
   (:address.postalCode ?address ?postalCode)
   (:address.city                ?city)
   (:address.streetAddress       ?streetAddress)
   (:address.region              ?region))

;;[firstName lastName postalCode city streetAddress region]
;;"Eleni" "Papadopoulou" "17562" "ATHENS" "Ahilleos" "Attika"


;;Summary
;;Here the joins are made in a chain also(addrees is object)
;;The s-expressions make it easier to read and see the join variables
;;because you ignore the relation,and you read key-value pairs
