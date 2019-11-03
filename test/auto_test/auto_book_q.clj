(ns auto-test.auto-book-q
  (:gen-class)
  (:use louna.louna
        louna.louna-util)
  (:require auto-test.test-run
            [clojure.core.match :refer [match]]))

(set-join-method "disk")
;;---------------------------functions they use------------------------------------

(defn average [numbers]
  (/ (apply + numbers) (count numbers)))

(defn add-commas [v]
  (reduce (fn [s m] (str s m)) "" (interleave v (repeat ","))))

;;-----------------------------------------------------------------------------------


(time
  (def q-map
    {
     ;;---------------------------------02---------------------------------------------
     [1]
     ;;sparqli was _query=ex003.rq
     (q {:q-in ["ex002"] :q-out ["print"]}

        [?craigEmail]
        (:ab.email :ab.craig ?craigEmail))

     [2]
     ;;sparql_query=ex006.rq
     (q {:q-in ["ex002"] :q-out ["print"]}

        [?craigEmail]
        (:ab.email :ab.craig ?craigEmail))

     [3]
     ;;sparql_query=ex008.rq
     (q {:q-in ["ex002"] :q-out ["print"]}
        [?person]
        (:ab.homeTel ?person "(229) 276-5135"))

     ;;-----------------------------------012------------------------------------------------------------------------
     [4]
     ;;spaql_query=ex008.rq
     (q {:q-in ["ex012"] :q-out ["print"]}
        [?person]
        (:ab.homeTel ?person "(229) 276-5135"))

     [5]
     ;sparql_query=ex013.rq
     (q {:q-in ["ex012"] :q-out ["print"]}
        [?craigEmail]
        (:ab.firstName ?person "Craig")
        (:ab.email ?person ?craigEmail))

     [6]
     ;;sparql_query=ex015.rq
     (q {:q-in ["ex012"] :q-out ["print"]}
        [?craigEmail]
        (:ab.firstName ?person "Craig")
        (:ab.lastName "Ellis")
        (:ab.email ?craigEmail))

     [7]
     ;;sparql_query=ex017.rq (sto biblio to lei 047.rq tipo)
     (q {:q-in ["ex012"] :q-out ["print"]}
        [?first ?last]
        (:ab.homeTel ?person "(229) 276-5135")
        (:ab.firstName ?first)
        (:ab.lastName ?last))

     [8]
     ;;ex023.rq  (prepei 0 apotelesmata)
     (q {:q-in ["ex012"] :q-out ["print"]}

        [?craigEmail ?homeTel]
        (:ab.firstName ?person "Craig")
        (:ab.lastName "Ellis")
        (:ab.email ?craigEmail)
        (:ab.homeTel ?homeTel))


     ;;-------------------------------054----------------------------------------------------------------------------

     [9]
     ;;sparql_query=ex055.rq  (workTel exei mono enas , oi alli dio exoun homeTel )
     (q {:q-in ["ex054"] :q-out ["print"]}
        [?first ?last ?workTel]
        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:ab.workTel ?workTel))


     ;;-----------------------------069------------------------------------------------------------------------------

     [10]
     ;;sparql_query=ex070.rq
     (q {:q-in ["ex069"] :q-out ["print"]}
        [?last ?first ?CourseName]
        (:ab.firstName ?student ?first)
        (:ab.lastName ?last)
        (:ab.takingCourse ?course)
        (:ab.courseTitle ?course ?CourseName))


     [11]
     ;;sparql_query=ex094.rq
     (q {:q-in ["ex069"] :q-out ["print"]}
        [distinct ?first ?last]
        (:ab.firstName ?student ?first)
        (:ab.lastName ?last)
        (:ab.takingCourse ?course))

     ;;---------------------------104--------------------------------------------------------------------------------

     [12]
     ;;sparql_query=ex105.rq
     (q {:q-in ["ex104"] :q-out ["print"]}
        [?item ?cost]
        (:dm.cost ?item ?cost)
        (< ?cost 10))

     [13]
     ;;sparql_query=ex111.rq
     (q {:q-in ["ex104"] :q-out ["print"]}
        [?item ?cost ?location]
        (:dm.location ?item ?location)
        (:dm.cost ?cost)
        (contains? #{8 10 12} ?cost))

     [14]
     ;;sparql_query=ex112.rq
     (q {:q-in ["ex104"] :q-out ["print"]}

        [?s ?cost ?location]
        (:dm.location ?s ?location)
        (:dm.cost ?cost)
        (not (contains? #{:db.Montreal, :db.Lisbon} ?location)))

     [15]
     ;;sparql_query=ex109.rq
     (q {:q-in ["ex104"] :q-out ["print"]}

        [?s ?cost ?location]
        (:dm.location ?s ?location)
        (:dm.cost ?cost)
        (contains? #{:db.Montreal, :db.Lisbon} ?location))


     ;;--------------------------041------------------------------------------------------------------------------------

     [16]
     ;;sparql_query=ex086.rq (BLANK NODE)
     (q {:q-in ["ex041"] :q-out ["print"]}

        [?addressVal]
        (:ab.address ?s ?addressVal))

     [17]
     ;;sparql_query=ex088.rq
     (q {:q-in ["ex041"] :q-out ["print"]}

        [?firstName ?lastName ?streetAddress ?city ?region ?postalCode]
        (:ab.firstName ?person ?firstName)
        (:ab.lastName ?lastName)
        (:ab.address ?address)
        (:ab.postalCode ?address ?postalCode)
        (:ab.city ?city)
        (:ab.streetAddress ?streetAddress)
        (:ab.region ?region))


     ;;--------------------------------138-----------------------------------------------------------------------------
     [18]
     ;sparql_query=ex139.rq
     (q {:q-in ["ex138"] :q-out ["print"]}

        [?description ?amount ?tip ?total]
        (:e.description ?meal ?description)
        (:e.amount ?amount)
        ((* ?amount 0.2) ?tip)
        ((+ ?amount ?tip) ?total))

     [19]
     ;;sparql_query=ex141.rq
     (q {:q-in ["ex138"] :q-out ["print"]}

        [?mealCode ?amount]
        (:e.description ?meal ?description)
        (:e.amount ?amount)
        ((clojure.string/upper-case (subs ?description 0 3)) ?mealCode))


     ;;-------------union-test--------------------------------

     [20]
     ;sparql_query=ex098.rq
     (q {:q-in ["ex069"] :q-out ["print"]}
        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:add (:ab.courseTitle ?course ?courseName)))

     [21]
     ;;sparql_query=ex101.rq
     (q {:q-in ["ex100"] :q-out ["print"]}

        [?first ?last ?instrument]

        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:ab.instrument "trumpet")
        (:ab.instrument ?instrument)
        (:add (:ab.firstName ?person ?first)
          (:ab.lastName ?last)
          (:ab.instrument "sax")
          (:ab.instrument ?instrument)))

     [22]
     ;;sparql_query=ex103.rq
     (q {:q-in ["ex100"] :q-out ["print"]}

        [?first ?last ?instrument]

        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:ab.instrument ?instrument)
        ((:ab.instrument ?person "sax")
          (:add (:ab.instrument ?person "trumpet"))))

     [23]
     ;;sparql_query=1.rq
     (q {:q-in ["sp1"] :q-out ["print"]}

        [?title]
        (:dc10.title ?book ?title)
        (:add (:dc11.title ?book ?title)))

     [24]
     ;;sparql_query=2.rq
     (q {:q-in ["sp1"] :q-out ["print"]}

        [?x ?y]
        (:dc10.title ?book ?x)
        (:add (:dc11.title ?book ?y)))

     [25]
     ;;sparql_query=3.rq
     (q {:q-in ["sp1"] :q-out ["print"]}

        [?title ?author]
        (:dc10.title ?book ?title)
        (:dc10.creator ?author)
        (:add (:dc11.title ?book ?title)
          (:dc11.creator ?author)))


     ;;-----------------optional test---------------------------------------------

     [26]

     ;;sparql_query = 057.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last ?workTel]
        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:if (:ab.workTel ?person ?workTel)))

     [27]
     ;;sparql_query = 059.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last ?workTel ?nick]
        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:if (:ab.workTel ?person ?workTel)
          (:ab.nick ?nick)))

     [28]
     ;;sparql_query = 061.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last ?workTel ?nick]

        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:if (:ab.workTel ?person ?workTel))
        (:if (:ab.nick ?person ?nick)))


     ;;-------------------TESTING MINUS-------------------------

     [29]
     ;;sparql_query = 068.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last]
        (:ab.firstName ?person ?first)
        (:ab.lastName ?person ?last)
        (:not (:ab.workTel ?person ?workNum)))


     ;;--------Testing filter group--------------------------------------

     [30]
     ;;sparql_query=ex065.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last]
        (:ab.firstName ?person ?first)
        (:ab.lastName ?last)
        (:if (:ab.workTel ?person ?workNum))
        (= "nil" ?workNum))


     ;;---------------Testing sort-by--------------------------------------------------------

     [31]
     ;;sparql_query=ex146.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (:sort-by ?amount))

     [32]
     ;;sparql_query=148.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]

        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (:sort-by (desc ?amount)))


     [33]
     ;sparql_query=149.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (:sort-by ?description (desc ?amount)))

     [34]
     ;;sparql_query=151.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (:sort-by (desc ?amount))
        (:limit 1))

     [35]
     ;;sparql_query=153.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?maxAmount]
        (:e.amount ?meal ?amount)
        (:group-by ((apply max ?amount) ?maxAmount)))


     [36]
     ;;sparql_query=155.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?maxAmount]
        (:e.amount ?meal ?amount)
        (:group-by ((apply max ?amount) ?maxAmount))
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?maxAmount))

     [37]
     ;;sparql_query=156.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?avgAmount]
        (:e.amount ?meal ?amount)
        (:group-by ((average ?amount) ?avgAmount)))


     [38]
     ;;my test query(aggregate many columns)
     (q {:q-in ["ex145"] :q-out ["print"]}
        [?avgAmount]

        (:e.amount ?meal ?amount)
        ((+ 2 ?amount) ?newAmount)
        (:group-by ((average (concat ?amount ?newAmount)) ?avgAmount)))

     [39]
     ;;sparql_query = 158.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        (:e.amount ?meal ?amount)
        (:group-by ((reduce (fn [s m] (str s m)) "" (interleave ?amount (repeat ","))) ?amountList)))

     [40]
     ;;sparql_query = 158.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        (:e.amount ?meal ?amount)
        (:group-by ((add-commas ?amount) ?amountList)))

     [41]
     ;;sparql_query=160.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description]
        (:e.description ?meal ?description)
        (:e.amount ?amount)
        (:group-by ?description))

     [42]
     ;;sparql_query=164.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?mealTotal]
        (:e.description ?meal ?description)
        (:e.amount ?amount)
        (:group-by ?description
          ((apply + ?amount) ?mealTotal)
          (> ?mealTotal 20)))

     [43]
     ;;subqueries
     ;;ex137.rq
     (q {:q-in ["ex069"] :q-out ["print"]}

        (:ab.lastName ?student ?lastName)
        (:project ?lastName)
        (:ab.courseTitle ?course ?courseName))

     [44]
     ;;my test
     (q {:q-in ["ex041"] :q-out ["print"]}

        (:ab.firstName ?person ?firstName)
        (:ab.lastName ?lastName)
        (:ab.address ?address)
        (:ab.postalCode ?address ?postalCode)
        (:ab.postalCode ?address ?postalCode)
        (:ab.city ?city)
        (:ab.streetAddress ?streetAddress)
        (:ab.region ?region)

        ;;filters
        (= ?person :ab.i0432)                               ;;1-var-filter
        (= ?city "Springfield")                             ;;1-var-filter
        (and (= ?person :ab.i0432) (= ?lastName "Mutt"))    ;;relation-filter
        (and (= ?firstName "Richard") (= ?lastName "Mutt")) ;;table filter se relation-vars

        ;;binds+filters
        ((str ?city) ?cityB)                                ;;bind stin proti
        (= ?cityB "Springfield")                            ;;filtro sto bind tis protis
        ((str ?firstName ?lastName) ?firstlast)             ;;bind
        (= ?firstlast "RichardMutt")                        ;;filtro sto bind

        ;;bind on binds+filter
        ((str ?firstlast ?city) ?firstlastcity)             ;;bind on bind
        (= ?firstlastcity "RichardMuttSpringfield")         ;;filtro sto bind on bind

        ;;table filter se relation vars+binds
        (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?firstlast "RichardMutt"))

        ;;table filter se relation vars + binds on binds
        (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?firstlastcity "RichardMuttSpringfield")))


     ;;-------------------------testing ?r--------------------------------------------
     [45]
     ;;my test-query
     (q {:q-in ["ex069"] :q-out ["print"]}
        (?r ?s ?o))

     ;;to bug einai oti exo idia var,to r exi 2 ids
     [46]
     ;;my test-query
     (q {:q-in ["ex069"] :q-out ["print"]}
        (?r ?s ?o)
        (or (= ?r :ab.lastName) (= ?r :ab.firstName)))

     [47]
     ;;sparql_query=ex010.rq
     (q {:q-in ["ex002"] :q-out ["print"]}
        [?propertyName ?propertyValue]

        (?propertyName :ab.cindy ?propertyValue))

     [48]
     ;;sparql_query=ex019.rq
     (q {:q-in ["ex012"] :q-out ["print"]}

        [?propertyName ?propertyValue]
        (:ab.firstName ?person "Cindy")
        (:ab.lastName "Marshall")
        (?propertyName ?propertyValue))

     [49]
     ;;ex021.rq
     (q {:q-in ["ex012"] :q-out ["print"]}

        [?s ?p ?o]
        (?p ?s ?o)
        (not (= (.indexOf ?o "yahoo") -1)))

     [50]
     ;;ex090
     (q {:q-in ["ex069"] :q-out ["print"]}

        [?p]
        (?p ?s ?o))

     [51]
     ;;ex092 (biblio exi tipo lathos)
     (q {:q-in ["ex069"] :q-out ["print"]}

        [distinct ?p]
        (?p ?s ?o))

     [52]
     ;;ex123.rq (multiple rdf-files))
     (q {:q-in ["ex122-123"] :q-out ["print"]}
        [?email]
        (:ab.email ?s ?email))

     [53]
     ;;my-test(testing auto-splitting BGP)
     (q {:q-in ["ex041"] :q-out ["print"]}
        (:ab.firstName ?person ?firstName)
        (:ab.lastName ?lastName)
        (:ab.address ?address)
        (:ab.postalCode ?address ?postalCode)
        (:ab.city ?city)
        ((str ?firstName ?lastName) ?flname)
        ((str ?flname ?lastName) ?po)
        (and (= ?person :ab.i0432) (= ?lastName "Mutt"))
        (= ?flname "RichardMutt")
        (and (= ?firstName "Richard") (= ?lastName "Mutt"))
        (and (= ?firstName "Richard") (= ?lastName "Mutt") (= ?flname "RichardMutt"))


        (:ab.firstName ?xperson ?xfirstName)
        (:ab.lastName ?xlastName)
        (:ab.address ?xaddress)
        (:ab.postalCode ?xaddress ?xpostalCode)
        (:ab.city ?xcity)

        (and (= ?xperson :ab.i0432) (= ?firstName "Richard") (= ?xfirstName "Richard"))
        ((str ?firstName ?xfirstName) ?lo)
        (= ?lo "RichardRichard"))

     [54]
     ;;sparql_query = 063.rq
     (q {:q-in ["ex054"] :q-out ["print"]}

        [?first ?last]
        (:ab.lastName ?person ?last)
        (:if (:ab.nick ?person ?first))
        (:if (:ab.firstName ?person ?first)))

     [55]
     ;;sparql_query=ex496.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (contains? #{"lunch" "dinner"} ?description))

     [56]
     ;;sparql_query=ex498.rq
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (contains? #{["2011-10-15" "lunch"]
                     ["2011-10-16" "dinner"]} [?date ?description]))

     [57]
     ;;sparql_query=ex500.rq(xrisimopoei to clojure.core.match gia to _ pou antistixi sto UNDEF tis sparql)
     (q {:q-in ["ex145"] :q-out ["print"]}

        [?description ?date ?amount]
        (:e.description ?meal ?description)
        (:e.date ?date)
        (:e.amount ?amount)
        (match [?date ?description]
               [_ "lunch"] true
               ["2011-10-16" _] true
               :else false))

     [58]
     ;;sparql_query=1.rq
     (q {:q-in ["spValues1"] :q-out ["print"]}

        [?book ?title ?price]
        (:dc11.title ?book ?title)
        (:ns.price ?book ?price)
        (contains? #{:book.book1 :book.book3} ?book))


     [59]
     ;;sparql_query=2.rq
     (q {:q-in ["spValues1"] :q-out ["print"]}

        [?book ?title ?price]
        (:dc11.title ?book ?title)
        (:ns.price ?book ?price)
        (match [?book ?title]
               [_ "SPARQL Tutorial"] true
               [book:book2 _] true
               :else false))

     [60]
     (q {:q-in ["ex012"] :q-out ["print"]}

        [?person ?p ?o]

        (:ab.firstName ?person "Craig")
        (:ab.lastName "Ellis")
        (?p ?person ?o))

     }))


#_(do (auto-test.test-run/save-q-map q-map "auto-book-q")
    (println "Test results saved."))

(auto-test.test-run/test-q-map q-map "auto-book-q")
