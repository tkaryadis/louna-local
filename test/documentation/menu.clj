(ns documentation.menu
  (:use louna.louna
        louna.louna-util))

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

;;used in why louna?
(defn tip [cost ratio] (* cost ratio))
(defn get-meals [mealtype maxcost tipratio]
   (q {:q-in [menuDB] :q-out ["print"]}
      [?name]
      (:meal.type    ?meal  mealtype)
      (:meal.name           ?name)
      (:meal.cost           ?cost)
      ((tip ?cost tipratio) ?tip)
      ((+ ?cost ?tip)       ?totalcost)
      (<= ?totalcost maxcost)))

(get-meals "dinner" 20 0.1)



;;Its dinner time and you want to see all the meals served
#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal "dinner"))

;;You dont want the meal-codes you want the meal names
#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal "dinner")
   (:meal.name ?meal ?name))

;;you want to see the costs also of the "dinner" meals
#_(q {:q-in [menuDB] :q-out ["print"]}
   [?name ?cost]
   (:meal.type ?meal "dinner")
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost))

;;You have only 20euro so you want cost<=20(bye "shrimp soup")
#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal "dinner")
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (<= ?cost 20))

;;You have only 20euro so you want cost<=20(bye "shrimp soup")
#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal "dinner")
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (<= ?cost 20)
   (not (= ?name "pasta bolonese")))


;;Then you think that you need to give a tip at least 10%(bye pasta bolonese)
#_(q {:q-in [menuDB] :q-out ["print"]}

   [?name ?totalcost]

   (:meal.type ?meal "dinner")
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   ((* ?cost 0.1)    ?tip)
   ((+ ?cost ?tip)   ?totalcost)
   (<= ?totalcost 20))

#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal ?type)
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (:sort-by   ?cost))

#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal ?type)
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (:group-by ?type))

#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal ?type)
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (:group-by ?type
      ((apply + ?cost) ?groupCost)
      (< ?groupCost 40)))

#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal ?type)
   (:meal.name ?meal ?name)
   (:meal.cost ?meal ?cost)
   (:group-by ((count ?meal) ?numberOfMeals)))


#_(q {:q-in [menuDB] :q-out ["print"]}
   (:meal.type ?meal ?type)
   (:project distinct ?type))

#_(q {:q-in [menuDB] :q-out ["print"]}
   [distinct ?type]
   (:meal.type ?meal ?type)
   (:limit 1))