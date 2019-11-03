(ns bgp.binds-process
  (:require bgp.functions-process))


;out = vector of binds
(defn add-binds [table-info]
  (let [bind-queries (get table-info "bind-queries")
        relation-vars (get table-info "vars")
        bind-vars (get table-info "bind-vars")
        vars-filter-functions (get table-info "vars-filter-functions")]
    (loop [bind-queries bind-queries
           index 0  ;;remember bind order
           binds {}]
      (if (empty? bind-queries)
        (-> table-info
            (assoc "binds" (into [] (sort-by (fn [bind] (get bind "index"))
                                             (vals binds))))
            (dissoc "bind-queries")
            (dissoc "vars")
            (dissoc "bind-vars"))
        (let [bind-query (first bind-queries)
              bind-var (get bind-query 0)
              bind-code (get bind-query 1)
              dep-vars (library.util/get-vars-query-str bind-code)
              table-dep-vars (reduce (fn [relation-dep-vars dep-var]
                                       (if (contains? relation-vars dep-var)
                                         (conj relation-dep-vars dep-var)
                                         (clojure.set/union relation-dep-vars (get-in binds [dep-var "relation-dep-vars"]))))
                                     #{}
                                     dep-vars)
              f-vector (bgp.functions-process/get-bind-function bind-code)
              f-map (bgp.functions-process/vector-to-f-eval f-vector)
              filters-vec (get vars-filter-functions #{bind-var} [])
              filters (map bgp.functions-process/vector-to-f-eval filters-vec)]
          (recur (rest bind-queries)
                 (inc index)
                 (assoc binds bind-var  { "bind-var" bind-var
                                        ;"query" bind-query
                                        "index" index
                                        "dep-vars" dep-vars
                                        "relation-dep-vars" table-dep-vars
                                        "f-map" f-map
                                        "filters" filters})))))))

;;thelo na katebeno tis relations,otan mporo na balo bind na to/ta bazo + vazo tin/tis bind-vars stis idi relations

;;1)kano tin dep-vars iposinolo ton sorted-vars
;;  pernontas metablites apo tis tree-vars me tin seira eos otou isxiei
;;2)brisko index teleuteas koinis metablitis,bazo amesos meta
(defn add-bind-var [sorted-vars binds bind]
  (let [bind-var (get bind "bind-var")
        dep-vars-list (into [] (get bind "dep-vars"))
        max-dep-index (apply max (map (partial library.util/get-var-position sorted-vars) dep-vars-list))
        final-max (if (= max-dep-index 0) 1 max-dep-index)
        new-sorted-vars (into [] (concat (subvec sorted-vars 0 (+ final-max 1))
                                         [bind-var]
                                         (subvec sorted-vars (+ final-max 1) (count sorted-vars))))]
    new-sorted-vars))

(defn add-bind-vars-to-sorted-vars [table-info]
  (let [relation-sorted-vars (get table-info "sorted-vars")
        binds       (get table-info "binds")]
    (loop [binds binds
           sorted-vars relation-sorted-vars]
      (if (empty? binds)
        (assoc table-info "sorted-vars" sorted-vars "final-sorted-vars" sorted-vars)
        (recur (rest binds) (add-bind-var sorted-vars binds (first binds)))))))

;;---------------------------------------

;;to bind-exartate apo tin teleutea table-dep-var
(defn add-trigger-var-to-binds [table-info]
  (let [sorted-vars (get table-info "sorted-vars" )
        binds       (get table-info "binds")]
    (loop [binds binds
           binds-map {}]
      (if (empty? binds)
        (assoc table-info "binds" binds-map)
        (let [bind (first binds)
              relation-dep-vars (into [] (get bind "relation-dep-vars"))
              sorted-relation-dep-vars (sort-by (fn [v] (library.util/get-var-position sorted-vars v))
                                                relation-dep-vars)
              bind-last-dep (last sorted-relation-dep-vars)]
          (if (contains? binds-map bind-last-dep)
            (recur (rest binds) (assoc binds-map bind-last-dep (conj (get binds-map bind-last-dep) bind)))
            (recur (rest binds) (assoc binds-map bind-last-dep [bind]))))))))

;;------------------------------------------------------------------------

(defn add-binds-to-relations [table-info]
  (let [relations (get table-info "relations")
        sorted-vars (get table-info "sorted-vars")
        binds (get table-info "binds")]
    (loop [index 0
           relations relations]
      (if (= index (count relations))
        (dissoc (assoc table-info "relations" relations) "binds")
        (let [relation (get relations index)
              var-binds (into [] (apply concat
                                        (map (fn [v] (get binds v []))
                                             (sort-by (fn [v] (library.util/get-var-position sorted-vars v))
                                                      (get relation "add-vars")))))
              relation (assoc relation "binds" var-binds)
              ;- (if-not (empty? var-binds) (prn (get relation "query")))
              ]
          (recur (inc index) (assoc relations index relation)))))))