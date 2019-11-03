(ns documentation.binary-ops
   (:use louna.louna
         louna.louna-util))

#_(def personDB {
                :person.firstName
                [[1 "Richard"]
                 [2"Cindy"]
                 [3 "Craig"]]

                :person.lastName
                [[1 "Mutt"]
                 [2 "Marshall"]
                 [3 "Ellis"]]

                :person.country
                [[1 "GREECE"]
                 [2 "USA"]
                 [3 "SAUDI-ARABIA"]]

                :person.workTel
                [[3 "(245) 315-5486"]]

                :person.nickName
                [[1 "Dick"]]

                })


;;----------------and---------------------
#_(q {:q-in [personDB] :q-out ["print"]}
   (:person.firstName ?person ?first)
   (:person.lastName          ?last)
   (:person.country           ?country))

#_(q {:q-in [personDB] :q-out ["print"]}
   ((:person.firstName ?person ?first)
    (:person.lastName          ?last))
   (:and  (:person.country ?person ?country)))

#_(q {:q-in [personDB] :q-out ["print"]}
   ((:person.firstName ?person ?first)
      (:person.lastName          ?last))
   ((:person.country ?person ?country)))

;;-----------------if---------------------

#_(q {:q-in [personDB] :q-out ["print"]}
   (:person.firstName   ?person   ?first)
   (:person.lastName              ?last)
   (:person.workTel               ?workTel))

#_(q {:q-in [personDB] :q-out ["print"]}
   (:person.firstName    ?person   ?first)
   (:person.lastName               ?last)
   (:if (:person.workTel ?person   ?workTel)))


#_(q {:q-in [personDB] :q-out ["print"]}

   [?first ?last]
   (:person.lastName       ?person ?last)
   (:if (:person.nickName  ?person ?first)))

#_(q {:q-in [personDB] :q-out ["print"]}

   [?first ?last]
   (:person.lastName       ?person ?last)
   (:if (:person.nickName  ?person ?first))
   (:if (:person.firstName ?person ?first)))


;;-----------:not-------------------------------


#_(q {:q-in [personDB] :q-out ["print"]}
   (:person.firstName   ?person ?first)
   (:person.lastName            ?last)
   (:not (:person.workTel ?person ?workNum)))

;;-----------:add------------------------------

#_(def booksDB {
              :y2000.title
              [[2 "SPARQL Protocol Tutorial"]
               [3 "SPARQL (updated)"]]

              :y2010.title
              [[1 "SPARQL Query Language Tutorial"]
               [3 "SPARQL"]]
              })

#_(q {:q-in [booksDB] :q-out ["print"]}

   (:y2000.title       ?book ?title00)
   (:add (:y2010.title ?book ?title10)))