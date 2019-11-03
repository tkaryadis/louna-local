(ns bgp.relations-process
  (:require state.state
            bgp.functions-process))

(defn add-vars-info [table-info]
  (let [relation-queries-init (get table-info "relation-queries")
        bind-queries (get table-info "bind-queries")]
  (loop [relation-queries relation-queries-init
         vars []
         pairs []
         triples []]
    (if (empty? relation-queries)
      (assoc table-info
             "pairs" (into #{} pairs)
             "vars" (into #{} vars)
             "bind-vars" (into #{} (map first bind-queries))
             "triples" (into #{} triples))
      (let [query (first relation-queries)
            r (first query)
            s (second query)
            o (nth query 2)
            query-vars (into #{} (map library.util/get-var-name (filter library.util/qvar? query)))
            rel-type (library.util/get-relation-type query)
            r  (library.util/get-var-name r)
            s  (library.util/get-var-name s)
            o  (library.util/get-var-name o)]
        (cond
          (= rel-type "rso")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                (conj triples #{r s o}))
          (= rel-type "rs")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                 (conj triples #{r s}))
          (= rel-type "ro")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                 (conj triples #{r o}))
          (= rel-type "r")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                 (conj triples #{r}))
          (= rel-type "so")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 (conj pairs #{s o})
                 triples)
          (= rel-type "s")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                 triples)
          (= rel-type "o")
          (recur (rest relation-queries)
                 (concat vars query-vars)
                 pairs
                 triples)
          :else (do (prn "Unknown relation type") (System/exit 0))))))))


(defn get-1var-functions [vars-v vars-filter-functions]
  (reduce (fn [relation-filters v]
            (let [v-functions (get vars-filter-functions #{v} [])]
              (if (empty? v-functions)
                relation-filters
                (assoc relation-filters #{v} v-functions))))
          {}
          vars-v))

(defn get-2var-functions [vars-v vars-filter-functions]
  (let [[v1 v2 v3] vars-v
        subsets  [#{v1 v2} #{v1 v3} #{v2 v3}]]
    (reduce (fn [relation-filters subset]
              (let [v-functions (get vars-filter-functions subset [])]
                (if (empty? v-functions)
                  relation-filters
                  (assoc relation-filters subset v-functions))))
            {}
            vars-v)))

;;thelo to power-set ton vars
(defn get-relation-filters [vars vars-filter-functions]
  (let [vars-v (into [] vars)]
    (cond
      (= (count vars-v) 1)
      (get-1var-functions vars-v vars-filter-functions)
      (= (count vars-v) 2)
      (merge (get-1var-functions vars-v vars-filter-functions)
             (if (contains? vars-filter-functions vars)
               {vars (get vars-filter-functions vars)}
               {}))
      (= (count vars-v) 3)
      (merge (get-1var-functions vars-v vars-filter-functions)
             (get-2var-functions vars-v vars-filter-functions)
             (if (contains? vars-filter-functions vars)
               {vars (get vars-filter-functions vars)}
               {})))))

(defn get-var-val [v var-filters]
  (if (library.util/ttmp-var? v)
    (let [filter (get var-filters #{v})
          query-vector (first filter)
          trimed-val (.trim (get query-vector 2))
          val-str (subs trimed-val 0 (dec (.length trimed-val)))
          v-val (read-string val-str)]
      [v-val (dissoc var-filters #{v})])
    [nil var-filters]))

(defn relations-queries-to-relations [table-info]
  (let [relation-queries (get table-info "relation-queries")
        vars-filter-functions (get table-info "vars-filter-functions")]
    (loop [relation-queries relation-queries
           relations []]
      (if (empty? relation-queries)
        (-> table-info
            (assoc  "relations" relations)
            (dissoc "relation-queries")
            (dissoc "vars-filter-functions"))
        (let [query (first relation-queries)
              r (first query)
              rel-type (library.util/get-relation-type query)
              property (if (and (string? r) (not (library.util/qvar? r)))
                         (let [r-property-v (read-string r)]
                           {"prop-type"  (first r-property-v)
                            "prop-value" (rest r-property-v)})
                          nil)]
          (cond
            (= rel-type "s")
            (let [s-var (library.util/get-var-name (second query))
                  vars #{s-var}
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                                 {"query"    query
                                                  "rel-type" rel-type
                                                  "relation" r
                                                  "filters"  filters
                                                  "s-var"    s-var
                                                  "vars"     vars
                                                  "property" property})))

            (= rel-type "o")
            (let [o-var (library.util/get-var-name (nth query 2))
                  vars #{o-var}
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                                   {"query"    query
                                                    "rel-type" rel-type
                                                    "relation" r
                                                    "filters"  filters
                                                    "o-var"    o-var
                                                    "vars"     vars
                                                    "property" property})))
            (= rel-type "so")
            (let [s-var (library.util/get-var-name (second query))
                  o-var (library.util/get-var-name (nth query 2))
                  pair [s-var o-var]
                  vars (into #{} pair)
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                                   {"query" query
                                                    "rel-type" rel-type
                                                    "relation" r
                                                    "pair" pair
                                                    "filters" filters
                                                    "vars" vars
                                                    "property" property})))
            (= rel-type "rso")
            (let [r-var (library.util/get-var-name (first query))
                  s-var (library.util/get-var-name (second query))
                  o-var (library.util/get-var-name (nth query 2))
                  triple [r-var s-var o-var]
                  vars (into #{} triple)
                  filters (get-relation-filters vars vars-filter-functions)
                  [r-val filters] (get-var-val r-var filters)
                  [s-val filters] (get-var-val s-var filters)
                  [o-val filters] (get-var-val o-var filters)

                  rel-vals [r-val s-val o-val]

                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                             {"query" query
                                              "rel-type" rel-type
                                              "triple" triple
                                              "filters" filters
                                              "vars" vars
                                              "rel-vals" rel-vals})))
            (= rel-type "rs")
            (let [r-var (library.util/get-var-name (first query))
                  s-var (library.util/get-var-name (second query))
                  triple [r-var s-var]
                  vars (into #{} triple)
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                             {"query" query
                                              "rel-type" rel-type
                                              "triple" triple
                                              "filters" filters
                                              "vars" vars})))
            (= rel-type "ro")
            (let [r-var (library.util/get-var-name (first query))
                  o-var (library.util/get-var-name (nth query 2))
                  triple [r-var o-var]
                  vars (into #{} triple)
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                             {"query" query
                                              "rel-type" rel-type
                                              "triple" triple
                                              "filters" filters
                                              "vars" vars})))
            (= rel-type "r")
            (let [r-var (library.util/get-var-name (first query))
                  triple [r-var]
                  vars (into #{} triple)
                  filters (get-relation-filters vars vars-filter-functions)
                  filters (bgp.functions-process/get-filters-f-eval-map filters)]
              (recur (rest relation-queries) (conj relations
                                             {"query" query
                                              "rel-type" rel-type
                                              "triple" triple
                                              "filters" filters
                                              "vars" vars})))
            :else (do (prn "Unknown relation type") (System/exit 0))))))))

(defn add-cost-to-relation [q-info r-values]
  (loop [r-values r-values
         total-cost 0]
    (if (empty? r-values)
      total-cost
      (let [r (first r-values)
            r-cost (library.util/get-relation-nlines q-info r)]
        (recur (rest r-values) (+ total-cost r-cost))))))

(defn add-cost-to-relation-var [q-info relations]
  (loop [index 0
         relations relations]
    (if (= index (count relations))
      relations
      (let [relation (get relations index)
            qrelation? (contains? relation "triple")
            qrelation-single  (if qrelation?
                                (let [r-var (first (get relation "triple"))]
                                  (if (library.util/ttmp-var? r-var)
                                    (let [[r-val] (get relation "rel-vals")]
                                      r-val)
                                   nil))
                                nil)
            property (get relation "property" {})
            prelation?  (not (empty? property))]
        (cond

          qrelation-single
          (recur (inc index)
                 (assoc relations index
                                  (assoc relation
                                    "cost"
                                    (add-cost-to-relation q-info [qrelation-single]))))
          qrelation?
          (let [f-rel (get q-info "f-rel")
                m-rel (get q-info "m-rel")
                rel (concat f-rel m-rel)]
            (recur (inc index)
                   (assoc relations index
                                    (assoc relation
                                      "cost"
                                      (add-cost-to-relation q-info rel)))))

          prelation?
          (let [r-values (get property "prop-value")]
            (recur (inc index)
                   (assoc relations index
                                    (assoc relation
                                      "cost"
                                      (add-cost-to-relation q-info r-values)))))
          :else
          (recur (inc index)
                 (assoc relations index
                                  (assoc relation
                                    "cost"
                                    (add-cost-to-relation q-info [(get relation "relation")])))))))))


(defn add-cost-to-relations [q-info table-info]
  (let [relations (add-cost-to-relation-var q-info (get table-info "relations"))]
    (assoc table-info "relations" relations)))