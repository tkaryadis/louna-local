(ns book-qc.cq
  (:use louna.louna
        queries.q))


;;ex185.rq
#_(c {:q-in ["ex012"] :q-out ["print"]
    :c-out ["print"]}
   (:ab.areaCode ?person ?areaCode)
   (q (:ab.homeTel ?person ?phone)
      ((subs ?phone 1 4) ?areaCode)))

;;ex188.rq
#_(c {:q-in ["ex187"] :q-out ["print"]
    :c-out ["print"]}
     (:ab.hasGrandfather ?child ?gparent)
     (q (:ab.hasParent ?child    ?parent)
        (:ab.hasParent ?parent   ?gparent)
        (:ab.gender    ?gparent  :d.male)))


;;ex190.rq
#_(c {:q-in ["ex187"] :q-out ["print"]
    :c-out ["print"]}
     (:ab.hasAunt ?person ?aunt)
     (q (:ab.hasParent  ?person ?parent)
        (:ab.hasParent  ?parent ?gparent)
        (:ab.hasParent  ?aunt   ?gparent)
        (:ab.gender             :d.female)
        (not= ?parent ?aunt)))


;;ex203.rq
#_(c {:q-in ["ex198"] :q-out ["print"]
      :c-out ["print"]}
     (:dm.problem ?s         :dm.prob29)
     (:dm.prob29  :rdfs.label "Location value must be a URI.")
     (q (:dm.location   ?s  ?city)
        (not (keyword? ?city))))

;;ex205.rq
#_(c {:q-in ["ex198"] :q-out ["print"]
      :c-out ["print"]}
     (:dm.problem ?item :dm.prob32)
     (:rdfs.label :dm.prob32  "Amount must be an integer.")
     (q (:dm.amount ?item ?amount)
        (not (integer? ?amount))))

;;ex207.rq
#_(c {:q-in ["ex198"] :q-out ["print"]
      :c-out ["print"]}
     (:dm.problem ?item :dm.problem44)
     (:rdfs.label :dm.problem44 "Expenditures over 100 require grade 5 approval.")
     (q (:dm.cost   ?item ?cost)
        (:dm.amount ?item ?amount)
        (:if (:dm.approval ?item ?approvingEmployee)
          (:dm.jobGrade ?approvingEmployee ?grade))
        ((* ?cost ?amount) ?totalCost)
        (and (> ?totalCost 100)
             (or (= "nil" ?grade) (< ?grade 5)))))

;;ex209
#_(c {:q-in ["ex198"] :c-out ["print"]}
     (:dm.problem ?prob32item :dm.prob32)
     (:rdfs.label :dm.prob32 "Amount must be an integer.")
     (:dm.problem ?prob29item  :dm.prob29)
     (:rdfs.label :dm.prob29  "Location value must be a URI.")
     (:dm.problem ?prob44item  :dm.prob44)
     (:rdfs.label :dm.prob44 "Expenditures over 100 require grade 5 approval.")
     (:rdfs.label :dm.probXX "This is a dummy problem.")
     (q (:dm.amount ?prob32item ?amount)
        (not (integer? ?amount))
        (:add (:dm.location ?prob29item ?city)
              (not (keyword? ?city)))
        (:add (:dm.cost ?prob44item ?cost)
              (:dm.amount ?prob44item ?amount)
              (:if (:dm.approval ?item ?approvingEmployee)
                   (:dm.jobGrade ?approvingEmployee ?grade))
                   ((* ?cost ?amount) ?totalCost)
                   (and (> ?totalCost 100)
                        (or (= nil ?grade) (< ?grade 5))))))
