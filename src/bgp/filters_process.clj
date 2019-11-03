(ns bgp.filters-process
  (:require bgp.functions-process))


;;------------------seperate filter queries------------------------------------------

(defn seperate-filter-queries [vars bind-vars pairs triples filter-queries]
  (reduce (fn [[vars-filter-queries table-filter-queries] query]
            (let [vars (into #{} (map (fn [cur-var] (subs cur-var 1)) (re-seq #"\?\w+" (str query))))]
              (if (= (count vars) 1)
                (if (or (contains? vars (first vars)) (contains? bind-vars (first vars)))
                  [(conj vars-filter-queries query) table-filter-queries])
                (if (or (contains? pairs vars) (contains? triples vars))
                  [(conj vars-filter-queries query) table-filter-queries]
                  [vars-filter-queries (conj table-filter-queries query)]))))
          [[] []]
          filter-queries))

;;-------------------------------vars-filter-queries -> vars-filter-functions-------------------------------------

(defn get-vars-filter-functions [vars-filter-queries]
  (loop [vars-filter-queries vars-filter-queries
         relations-functions {}]
    (if (empty? vars-filter-queries)
      relations-functions
      (let [filter-query (first vars-filter-queries)
            [dep-vars vars-filter-fuction] (bgp.functions-process/get-vars-filter-function filter-query)
            new-vars-functions (if (contains? relations-functions dep-vars)
                                 (assoc relations-functions dep-vars (conj (get relations-functions dep-vars) vars-filter-fuction))
                                 (assoc relations-functions dep-vars [vars-filter-fuction]))]
        (recur (rest vars-filter-queries) new-vars-functions)))))

;;-------------------------Xorizi ta filtra se 2 katigories--------------------------------------------------------------------------
;;1)filtra pou efarmozonte kata tin anagnosi mia sxesis/dimiourgia enos bind (vars-filter-functions
;;  exoun key tis dep-vars
;;2)filtra pou efarmozonte otan bazo mia neta metabliti ston pinaka (table-filters)
;;  exoun key tin metabliti pou bazo kata to join

(defn seperate-filters [table-info]
  (let [vars (get table-info "vars")
        bind-vars (get table-info "bind-vars")
        pairs (get table-info "pairs")
        triples (get table-info "triples")
        filter-queries (get table-info "filter-queries")
        [vars-filter-queries table-filter-queries] (seperate-filter-queries vars bind-vars pairs triples filter-queries)
        vars-filter-functions (get-vars-filter-functions vars-filter-queries)]
    (-> table-info
        (assoc "vars-filter-functions"  vars-filter-functions)
        (assoc "table-filter-queries"   table-filter-queries)
        (dissoc "filter-queries")
        (dissoc "pairs")
        (dissoc "triples"))))


;;;-----------------table-filter-functions---------------------------------------------

;;i filter vars tha exoun os key tin teleutea metabliti
(defn get-table-filter-functions [filter-f-queries sorted-vars]
  (loop [filter-f-queries filter-f-queries
         table-functions {}]
    (if (empty? filter-f-queries)
      table-functions
      (let [filter-query (first filter-f-queries)
            [filter-var dep-vars table-filter-function] (bgp.functions-process/table-filter-query-to-function sorted-vars filter-query)
            new-table-functions (if (contains? table-functions filter-var)
                                 (let [prv-value (get table-functions filter-var)
                                       prv-dep-vars (get prv-value "dep-vars")
                                       new-dep-vars (clojure.set/union prv-dep-vars dep-vars)
                                       prv-functions (get prv-value "functions")
                                       new-functions (conj prv-functions table-filter-function)
                                       new-value {"dep-vars" new-dep-vars "functions" new-functions}]
                                   (assoc table-functions filter-var new-value))
                                 (assoc table-functions filter-var {"dep-vars" dep-vars "functions" [table-filter-function]}))]
        (recur (rest filter-f-queries) new-table-functions)))))

(defn add-trigger-var-to-table-filters [table-info]
  (let [table-filter-queries (get table-info "table-filter-queries")
        sorted-vars-plus-binds (get table-info "sorted-vars")
        table-filter-functions (get-table-filter-functions table-filter-queries sorted-vars-plus-binds)
        table-filter-functions (bgp.functions-process/get-table-filters-f-eval-map table-filter-functions)]
    (-> table-info
        (dissoc "sorted-vars")
        (dissoc "table-filter-queries")
        (assoc  "table-filter-functions" table-filter-functions))))