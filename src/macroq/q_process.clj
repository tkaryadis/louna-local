(ns macroq.q-process
  (:require library.util
            louna.louna-util
            bgp.functions-process
            macroq.q-autogroup
            macroq.q-rewrite
            macroq.q-process-triples
            macroq.q-process-op-args))


;;----------------run-q-------------------------------------------------------------------------------

(defn get-q-info [db constructs f-rel m-rel]
  (let [db-stats (if-not (= db "")
                   (read-string (slurp (str (state.db-settings/get-dbs-path) db "/" db ".stats")))
                   {})]
    {"db" db
     "db-stats" db-stats
     "constructs" constructs
     "f-rel" f-rel
     "m-rel" m-rel}))

(defn get-db [settings]
  (let [q-in (get settings :q-in [])
        db (first (filter string? q-in))]
    (if (nil? db)
      ""
      db)))

(defn get-c-in-constructs [settings]
  (let [c-in (filter #(not (string? %)) (get settings :q-in []))]
    (cond
      (empty? c-in)
      {}
      (= (count c-in) 1)
      (first c-in)
      :else
      (apply louna.louna-util/merge-relations c-in))))


;;-------------------------macro-q-------------------------------------------------------------------

(defn get-settings [queries]
  (if (map? (first queries))
    [(first queries) (rest queries)]
    [{} queries]))

(defn get-namespace-queries [queries]
  (if (and (map? (first queries))
           (keyword? (first (first (first queries)))))
    (let [ns-map (first queries)
          ns-prefixes (keys ns-map)
          q-namespaces (reduce (fn [new-ns-map prefix]
                                 (let [ns-space (get ns-map prefix)
                                       prefix (name prefix)]
                                   (assoc new-ns-map prefix ns-space)))
                               {}
                               ns-prefixes)]
      [q-namespaces  (rest queries)])
    [{} queries]))

(defn get-project [queries]
  (if (vector? (first queries))
    [(first queries) (rest queries)]
    [['*] queries]))

;;-----------------------------------------------------------------------------
;;Make triples to ?r ?s ?o by adding filters + temporary vars

(defn triple-to-rso [query]
  (let [query (rest query)
        [filters query] (reduce
                          (fn [[filters query index] member]
                            (if (clojure.string/starts-with? member "?")
                              [filters (conj query member) (inc index)]
                              (let [temp-var (state.state/get-new-var-ID)
                                    temp-var (str "?T" (subs temp-var 1))]
                                (if-not false                    ;(= index 0)
                                  [(conj filters (macroq.q-process-triples/make-query-str (list '= temp-var member)))
                                   (conj query temp-var) (inc index)]
                                  (let [member-str (name member)
                                        member-str (clojure.string/replace member-str #"\." "ATT")
                                        member-str (str "PRE" member-str)
                                        temp-var (str temp-var member-str)]
                                    [(conj filters (macroq.q-process-triples/make-query-str (list '= temp-var member)))
                                     (conj query temp-var) (inc index)])))))
                          [[] [] 0]
                          query)]
    [filters [(into [] (concat (list 1) query))]]))

(defn find-bgp [group]
  (loop [group group
         bgp []]
    (let [query (first group)]
      (if (or (nil? query) (library.util/group? query))
        [(macroq.q-autogroup/get-groups bgp) group]
        (if (or (macroq.q-rewrite/property-path-query? query) (macroq.q-rewrite/or-property-path? query)
                (macroq.q-rewrite/star-property-path? query))
          (let [group (macroq.q-rewrite/property-paths group)
                query (macroq.q-process-triples/make-query-str (first group))]
            (recur (rest group) (conj bgp query)))
          (let [query (macroq.q-process-triples/make-query-str query)
                [filters queries] (if-not (= (first query) 1)
                                      [[] [query]]
                                      (triple-to-rso query))
                fq (into [] (concat queries filters))]
            (recur (rest group) (apply (partial conj bgp) fq))))))))

(defn find-bgp-disk [group]
  (loop [group group
         bgp []]
    (let [query (first group)]
      (if (or (nil? query) (library.util/group? query))
        [(macroq.q-autogroup/get-groups bgp) group]
        (if (macroq.q-rewrite/property-path-query? query)
          (let [group (macroq.q-rewrite/property-paths group)
                query (macroq.q-process-triples/make-query-str (first group))]
            (recur (rest group) (conj bgp query)))
          (recur (rest group) (conj bgp (macroq.q-process-triples/make-query-str query))))))))

(defn add-str-group [group]
  (loop [group group
         str-group []]
    (if (empty? group)
      str-group
      (let [group (-> group
                      macroq.q-rewrite/add-and-operator
                      macroq.q-rewrite/multiple-var-binds)
            query (first group)]
        (if-not (library.util/group? query)                 ;;bgp starts
          (let [[bgp-str group] (if (= (state.db-settings/get-join-method) "disk")
                                  (find-bgp-disk group)
                                  (find-bgp group))]
            (recur group (apply (partial conj str-group) bgp-str)))
          (let [operator (library.util/change-operator-name (str (first query)))]
            (cond
              (= operator "__table__")
              (let [table-str (macroq.q-process-op-args/process-table-args query)]
                (recur (rest group) (conj str-group table-str)))
              (= operator "__project__")
              (let [project-str (macroq.q-process-op-args/process-project-args query)]
                (recur (rest group) (conj str-group project-str)))
              (= operator "__sort-by__")
              (let [sort-by-str (macroq.q-process-op-args/process-sort-by-args query)]
                (recur (rest group)  (conj str-group sort-by-str)))
              (= operator "__group-by__")
              (let [group-by-str (macroq.q-process-op-args/process-group-by-args query)]
                (recur (rest group)  (conj str-group group-by-str)))
              (or (= operator "__do__") (= operator "__do-each__"))
              (let [do-str (macroq.q-process-op-args/process-do-args query)]
                (recur (rest group)  (conj str-group do-str)))
              :else    ;;its binary operator run for second argument
              (let [new-group (into [] (concat (list operator) (add-str-group (rest query))))] ;;recursion
                (recur (rest group)  (conj str-group new-group))))))))))