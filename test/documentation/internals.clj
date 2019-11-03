(ns documentation.internals
  (:use louna.louna
        louna.louna-util
        documentation.shared-functions))

;;Used in BGP tutorial
(def menuDB {:meal.name
             [[1 "eggs with potatoes"]
              [2 "fried rice with eggs"]
              [3 "chicken soup"]
              [4 "shrimp soup"]
              [5 "pasta bolonese"]
              [6 "pizza"]]

             :meal.type
             [[1 "breakfast"]
              [2 "lunch"]
              [3 "lunch"]
              [4 "dinner"]
              [5 "dinner"]
              [6 "dinner"]]

             :meal.cost
             [[1 6.5]
              [2 11]
              [3 20]
              [4 28]
              [5 19.5]
              [6 12]]
             })

;;used for the query plan
#_(q {:q-in ["ex041"] :q-out ["print"]}

     (:ab.firstName ?person ?firstName)
     (:ab.lastName ?lastName)
     (:ab.address ?address)
     (:ab.postalCode ?address ?postalCode)
     (:ab.postalCode ?address ?postalCode)
     (:ab.city ?city)
     (:ab.streetAddress ?streetAddress)
     (:ab.region ?region)

     ;;filters
     (= ?person :ab.i0432)                                 ;;1-var-filter
     (= ?city "Springfield")                               ;;1-var-filter
     (and (= ?person :ab.i0432) (= ?lastName "Mutt"))      ;;relation-filter
     (and (= ?firstName "Richard") (= ?lastName "Mutt"))   ;;table filter se relation-vars

     ;;binds+filters
     ((str ?city) ?cityB)                                  ;;bind stin proti
     (= ?cityB "Springfield")                              ;;filtro sto bind tis protis
     ((str ?firstName ?lastName) ?firstlast)               ;;bind
     (= ?firstlast "RichardMutt")                          ;;filtro sto bind

     ;;bind on binds+filter
     ((str ?firstlast ?city) ?firstlastcity)               ;;bind on bind
     (= ?firstlastcity "RichardMuttSpringfield")           ;;filtro sto bind on bind

     ;;table filter se relation vars+binds
     (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?firstlast "RichardMutt"))

     ;;table filter se relation vars + binds on binds
     (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?firstlastcity "RichardMuttSpringfield")))


;;Used in BGP tutorial
#_(q {:q-in [menuDB] :q-out ["print"]}

     [?name ?totalcost]
     (:meal.type ?meal "dinner")
     (:meal.name ?meal ?name)
     (:meal.cost ?meal ?cost)
     ((* ?cost 0.1)    ?tip)
     ((+ ?cost ?tip)   ?totalcost)
     (<= ?totalcost 20))


;;used-in-group-example
#_(q {:q-in ["ex100"] :q-out ["print"]}

     [?first ?last ?instrument]

     (:ab.firstName ?person ?first)
     (:ab.lastName ?last)
     (:ab.instrument ?instrument)
     ((:ab.instrument ?person "sax")
       (:add (:ab.instrument ?person "trumpet")))
     (:sort-by (desc ?first)))


;;Used in auto-grouping tutorial
#_(q {:q-in ["ex041"] :q-out ["print"]}
   (:ab.firstName ?person   ?firstName)
   (:ab.lastName            ?lastName)
   (:ab.address             ?address)
   (:ab.postalCode ?address ?postalCode)
   (:ab.city                ?city)
   ((str ?firstName ?lastName) ?flname)
   ((str ?flname ?lastName) ?po)
   (and (= ?person :ab.i0432) (= ?lastName "Mutt"))
   (= ?flname "RichardMutt")
   (and (= ?firstName "Richard") (= ?lastName "Mutt"))
   (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?flname "RichardMutt"))


   (:ab.firstName ?xperson  ?xfirstName)
   (:ab.lastName            ?xlastName)
   (:ab.address             ?xaddress)
   (:ab.postalCode ?xaddress ?xpostalCode)
   (:ab.city                ?xcity)

   (and (= ?xperson :ab.i0432) (= ?firstName "Richard") (= ?xfirstName "Richard"))
   ((str ?firstName ?xfirstName) ?lo)
   (= ?lo "RichardRichard"))

;;used in auto-grouping tutorial
#_(q {:q-in ["ex145"] :q-out ["print"]}

   [?description ?date ?maxAmount]
   (:e.amount ?meal ?amount)
   (:group-by ((apply max ?amount) ?maxAmount))
   (:e.description ?meal ?description)
   (:e.date ?date))


;;Used in functions tutorial
#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.cost ?meal ?cost)
   ((* ?cost 0.1)    ?tip)
   (< (+ ?cost ?tip) 20))


;;Used in functions tutorial
(defn f [euro]
  (q {:q-in [menuDB] :q-out ["print"]}
     (:meal.cost ?meal ?cost)
     ((* ?cost 0.1)    ?tip)
     (< (+ ?cost ?tip) euro)))

(f 10)