(ns bgp.joins-process
  (:require clojure.set))

(defn sort-add-vars [relation]
  (let [join-vars (get relation "join-vars")
        add-vars (get relation "add-vars")]
    (cond
      (< (count add-vars) 2)
      (into [] add-vars)

      (= (count add-vars) 3)
      (get relation "triple")

      (= (count add-vars) 2)
      (if (contains? relation "triple")
        (let [triple (get relation "triple")
              triple (into [] (filter #(not (contains? join-vars %)) triple))]
          triple)
        (get relation "pair")))))

(defn find-join-index [index vars relations]
  (loop [index index
         joined? false]
    (if joined?                        ;;joined?
      (do                                                   ;(prn vars (get (get relations index) "vars"))
        index)
      (let [join-query (get relations (+ index 1))
            join-query-vars (get join-query "vars")
            join-vars (clojure.set/intersection vars join-query-vars)]
        (if (empty? join-vars)
          (recur (inc index) joined?)
          (recur (inc index) true))))))

;;xekinao apo tin proti ean kani join me tin epomeni tin afino stin thesi tis
;;alios brisko tin proti me tin opia join kai tin kano epomeni tis
(defn find-order [relations index]
  (loop [index index
         vars (get (get relations index) "vars")
         relations relations
         sorted-vars []]
    (if (= index (- (count relations) 1))  ;;i teleutea panta kani join me tis proigoumenes
      [relations sorted-vars]
      (let [join-index (find-join-index index vars relations)
            join-query (get relations join-index)
            join-query-vars (get join-query "vars")
            join-query (assoc join-query
                         "join-vars" (clojure.set/intersection vars join-query-vars)
                         "add-vars"   (clojure.set/difference join-query-vars vars))
            sorted-add-vars (sort-add-vars join-query)
            new-sorted-vars (apply (partial conj sorted-vars) sorted-add-vars)]
        (if (= join-index (+ index 1))
          (recur (inc index)
                 (clojure.set/union vars join-query-vars)
                 (assoc relations join-index join-query)
                 new-sorted-vars)
          (let [new-relations (into [] (concat (subvec relations 0 (+ index 1))
                                               (list join-query)
                                               (subvec relations (+ index 1) join-index)
                                               (subvec relations (+ join-index 1))))]
            (recur (inc index)
                   (clojure.set/union vars join-query-vars)
                   new-relations
                   new-sorted-vars)))))))


;;tha tin balo os filtro stin relation pou bazi tin teleutea var tis
(defn add-filter-relation [sorted-vars relations-filters relation]
  (let [relation-var (last (sort-by (fn [v] (library.util/get-var-position sorted-vars v))
                                    (get relation "vars")))]
    (loop [index 0
           found? false]
      (if found?
        index
        (let [cur-relation (get relations-filters index)
              cur-relation-add-vars (get cur-relation "add-vars")]
          (if (contains? cur-relation-add-vars relation-var)
            (recur index true)
            (recur (inc index) false)))))))


(defn add-filter-relations [sorted-vars relations]
  (loop [relations relations
         relations-filters []]
    (if (empty? relations)
      relations-filters
      (let [relation (first relations)
            add-vars (get relation "add-vars")]
        (if (empty? add-vars)
          (let [parent-index (add-filter-relation sorted-vars relations-filters relation)
                parent-relation (get relations-filters parent-index)
                prv-filter-relations (get parent-relation "filter-relations" [])
                new-filter-relations (conj prv-filter-relations relation)
                new-parent-relation (assoc parent-relation "filter-relations" new-filter-relations)
                new-relations-filters (assoc relations-filters parent-index new-parent-relation)]
            (recur (rest relations) new-relations-filters))
          (recur (rest relations) (conj relations-filters relation)))))))



(defn update-first-relation [join-order-relations sorted-vars]
  (let [first-relation (first join-order-relations)
        first-relation (assoc first-relation "join-vars" #{})
        first-relation (assoc first-relation "add-vars" (get first-relation "vars"))
        sorted-add-vars (sort-add-vars first-relation)]
    [(assoc join-order-relations 0 first-relation) (into [] (concat sorted-add-vars sorted-vars))]))




;;thelo mia joined order xoris cartesians kai me to xamilotero kostos
;;var-domains kratao mono gia tis joined variables
;;chain joins kalitera apo star joins
(defn order-queries [table-info]
  (let [relations (get table-info "relations")

        sorted-relations (into [] (sort-by (fn [query] (get query "cost")) relations))

        ;- (prn (doall (map (fn [r] (get r "cost")) sorted-relations)))

        [join-order-relations sorted-vars]  (find-order sorted-relations 0)

        [join-order-relations sorted-vars] (update-first-relation join-order-relations sorted-vars)

        ;- (prn  (reduce (fn [c r] (conj c (get r "add-vars"))) [] join-order-relations))

        ;- (prn "xx" (reduce (fn [c r] (conj c (get r "join-vars"))) [] join-order-relations))

        relations (add-filter-relations sorted-vars join-order-relations)

        ;- (library.print-data/print-query-plan relations true)

        table-info (-> table-info
                       (assoc "relations" relations)
                       (assoc "sorted-vars" sorted-vars))]
    table-info))