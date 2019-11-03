(ns macroq.q-process-op-args
  (:require macroq.q-process-triples))


;;-----------------------project-------------------------------------------------

(defn get-project-vars-limit [query]
  (if (coll? (second query))
    (let [project-vars (second query)
          limit (if (= (count query) 3) (nth query 2) -1)]
      [project-vars limit])
    (let [project-vars-limit (into [] (rest query))
          last-value (peek project-vars-limit)]
      (if (number? last-value)
        [(pop project-vars-limit) last-value]
        [project-vars-limit -1]))))

(defn get-project-args [query]
  (let [[project-vars limit] (get-project-vars-limit query)]
    (if (= (first project-vars) 'distinct)
      (if (= (str (second  project-vars)) "*")
        [[] [:distinct :all] limit]
        [(into [] (map (comp library.util/get-var-name str)) (rest project-vars))
         [:distinct]
         limit])
      (if (= (str (first project-vars)) "*")
        [[] [:all] limit]
        [(into [] (map (comp library.util/get-var-name str)) project-vars)
         []
         limit]))))

(defn process-project-args [query]
  (let [[project-vars project-options limit] (macroq.q-process-op-args/get-project-args query)
        project-str (into [] (concat (list "__project__")
                                     (list project-vars)
                                     (list project-options)
                                     (list limit)))]
    project-str))

;;-----------------------sort-by-------------------------------------------------

(defn get-desc-vector [sort-arg]
  (reduce (fn [[sort-arg desc-vector] arg]
            (if (coll? arg)
              (if (= (first arg) 'desc)
                [(conj sort-arg (second arg)) (conj desc-vector :desc)]
                (if (= (first arg) 'asc)
                  [(conj sort-arg (second arg)) (conj desc-vector :asc)]
                  [(conj sort-arg arg) (conj desc-vector :asc)]))
              [(conj sort-arg arg) (conj desc-vector :asc)]))
          [[] []]
          sort-arg))

(defn process-sort-by-args [query]
  (let [sort-arg (rest query)
        [sort-arg desc-vector] (get-desc-vector sort-arg)
        sort-arg (map (fn [qvar]
                        (if (library.util/qvar? qvar)
                          (library.util/get-var-name (str qvar))
                          (macroq.q-process-triples/qvars-to-str qvar)))
                      sort-arg)
        sort-by-str (into [] (concat (list "__sort-by__") sort-arg (list desc-vector)))]
    sort-by-str))

;;-----------------------group-by-------------------------------------------------

(defn seperate-group-by-arguments [args]
  (reduce (fn [[group-vars binds filters] arg]
            (if (library.util/qvar? arg)
              [(conj group-vars (library.util/get-var-name (str arg)))
               binds
               filters]
              (if (library.util/bind-query-s? arg)
                [group-vars (conj binds arg) filters]
                [group-vars binds (conj filters arg)])))
          [[] [] []]
          args))

(defn process-group-by-args [query]
  (let [args (rest query)
        [group-by-vars binds filters] (seperate-group-by-arguments args)
        binds  (into [] (map macroq.q-process-triples/bind-members-str binds))
        filters (into [] (map macroq.q-process-triples/qvars-to-str filters))
        group-by-str (into [] (concat (list "__group-by__") (list group-by-vars) (list binds) (list filters)))]
    group-by-str))

;;-----------------------table---------------------------------------------------

(defn process-table-args [query]
  (let [table-bgp (into [] (rest query))
        table (first table-bgp)]
    (if (coll? table)
      (let [table-str (into [] (macroq.q-process-triples/make-relation-members-str table))
            filter-binds (map macroq.q-process-triples/make-query-str (rest table-bgp))
            table-bgp-str (reduce conj ["__table__" table-str] filter-binds)]
        table-bgp-str)
      (let [filter-binds (map macroq.q-process-triples/make-query-str (rest table-bgp))
            table-bgp-str (reduce conj ["__table__" table] filter-binds)]
        table-bgp-str))))

;;---------------------do/do-each------------------------------------------------

(defn process-do-args [query]
  (let [operator (first query)
        internal-operator (if (= operator :do) "__do__" "__do-each__")
        code (second query)
        code-str (macroq.q-process-triples/make-query-str code)
        do-str (into [] (concat (list internal-operator) (list code-str)))]
    do-str))