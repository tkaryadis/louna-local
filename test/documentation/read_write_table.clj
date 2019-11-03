(ns documentation.read-write-table
  (:use louna.louna
        louna.louna-util
        documentation.shared-functions))

(def t (q (:table (:person ?pid ?firstname ?lastname ?city ?country ?continent))))

;;table->relations
#_(c {:c-in [t] :c-out ["print"]}
     (:person.fistname ?pid ?firstname)
     (:person.lastname ?pid ?lastname)
     (:person.city ?pid ?city)
     (:person.country ?pid ?country)
     (:person.continent ?pid ?continent))


;;table->relations->sorted-table
#_(q {:q-out ["print" "sorted-table"]}
   [?pid ?firstname ?lastname ?city ?country ?continent]
   (:person.fistname ?pid ?firstname)
   (:person.lastname ?pid ?lastname)
   (:person.city ?pid ?city)
   (:person.country ?pid ?country)
   (:person.continent ?pid ?continent)
   (:sort-by ?pid)
   (c {:c-in [t] :c-out ["print"]}
      (:person.fistname ?pid ?firstname)
      (:person.lastname ?pid ?lastname)
      (:person.city ?pid ?city)
      (:person.country ?pid ?country)
      (:person.continent ?pid ?continent)))



;;table(t)->relations->table->relations
#_(c {:c-out ["print"]}
   (:person.fistname ?pid ?firstname)
   (:person.lastname ?pid ?lastname)
   (:person.city ?pid ?city)
   (:person.country ?pid ?country)
   (:person.continent ?pid ?continent)
   (q [?pid ?firstname ?lastname ?city ?country ?continent]
      (:person.fistname ?pid ?firstname)
      (:person.lastname ?pid ?lastname)
      (:person.city ?pid ?city)
      (:person.country ?pid ?country)
      (:person.continent ?pid ?continent)
      (c {:c-in [t]}
         (:person.fistname ?pid ?firstname)
         (:person.lastname ?pid ?lastname)
         (:person.city ?pid ?city)
         (:person.country ?pid ?country)
         (:person.continent ?pid ?continent))))

;;table(memory)->relations->table->relations->sorted_table - "takis" (apo proto macroq) + write to file

#_(q {:q-out ["print" "sorted-table-deltakis"]}
   [?pid ?firstname ?lastname ?city ?country ?continent]
   (:person.fistname ?pid ?firstname)
   (:person.lastname ?pid ?lastname)
   (:person.city ?pid ?city)
   (:person.country ?pid ?country)
   (:person.continent ?pid ?continent)
   (:sort-by ?pid)
   (c (:person.fistname ?pid ?firstname)
      (:person.lastname ?pid ?lastname)
      (:person.city ?pid ?city)
      (:person.country ?pid ?country)
      (:person.continent ?pid ?continent)
      (q [?pid ?firstname ?lastname ?city ?country ?continent]
         (:person.fistname ?pid ?firstname)
         (:person.lastname ?pid ?lastname)
         (:person.city ?pid ?city)
         (:person.country ?pid ?country)
         (:person.continent ?pid ?continent)
         (not= ?firstname "takis")
         (c {:c-in [t]}
            (:person.fistname ?pid ?firstname)
            (:person.lastname ?pid ?lastname)
            (:person.city ?pid ?city)
            (:person.country ?pid ?country)
            (:person.continent ?pid ?continent)))))