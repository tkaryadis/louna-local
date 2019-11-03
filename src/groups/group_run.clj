(ns groups.group-run
  (:require groups.table-binary-op
            groups.table-unary-op
            bgp.bgp-process))

;;;-----------------------------------RUN-GROUP--------------------------------------------------------

;;prepei na figi/allaxi aforouse otan ixa mixed se 1 group triples+alla groups(alla akoma douleuei)
(defn split-group [group]
  (loop [group group
         groups []
         cur-group []]
    (if (empty? group)
      (if-not (empty? cur-group) (conj groups cur-group) groups)
      (let [query (first group)]
        (if (library.util/triple? query)
          (recur (rest group) groups (conj cur-group query))
          (recur (rest group)
                 (if-not (empty? cur-group) (conj (conj groups cur-group) [query]) (conj groups [query]))
                 []))))))


(defn run-group [q-info group]
  (loop [groups (split-group group)
         table {"sorted-vars" []
                "table"       {}}]
    (if (empty? groups)
      table
      (let [cur-group (first groups)]
        (if (library.util/basic-pattern? cur-group)
          (recur (rest groups)
                 (bgp.bgp-process/process-bgp q-info cur-group))
          (let [new-group (first cur-group)
                operator (str (first new-group))
                new-group (into [] (rest new-group))]
            (if (library.util/binary-op? operator)
              (let [new-table (run-group q-info new-group)]
                (cond
                  (= operator "__add__") (recur (rest groups) (groups.table-binary-op/union table new-table))
                  (= operator "__if__") (recur (rest groups) (groups.table-binary-op/optional table new-table))
                  (= operator "__not__") (recur (rest groups) (groups.table-binary-op/minus table new-table))
                  (= operator "__and__") (recur (rest groups) (groups.table-binary-op/join table new-table))
                  :else (do (println "unknown operator") (recur (rest groups) table))))
              (cond
                (= operator "__table__")
                (let [table (first new-group)
                      filter-binds (rest new-group)
                      bind-queries (library.util/get-filter-bind-queries filter-binds 2)
                      bind-queries (map (fn [[bind-var bind-code]]
                                          (list bind-var bind-code))
                                        bind-queries)
                      filter-queries (map first (library.util/get-filter-bind-queries filter-binds 3))]
                  (if-not (map? table)
                    (let [table-name (name (first table))
                          sorted-vars (into [] (map (fn [v]
                                                      (let [v (str v)]
                                                        (if (= v "-")
                                                          v
                                                          (subs v 1))))
                                                    (rest table)))]
                      (recur (rest groups) (groups.table-unary-op/read-table table-name sorted-vars bind-queries filter-queries)))
                    (recur (rest groups) (groups.table-unary-op/filter-bind table bind-queries filter-queries))))
                (= operator "__fb__")
                (let [queries new-group
                      bind-queries (library.util/get-filter-bind-queries queries 2)
                      bind-queries (map (fn [[bind-var bind-code]]
                                           [bind-var bind-code])
                                         bind-queries)
                      filter-queries (map first (library.util/get-filter-bind-queries queries 3))]
                  (recur (rest groups) (groups.table-unary-op/filter-bind table bind-queries filter-queries)))

                (= operator "__sort-by__")
                (let [desc-vector (last new-group)
                      sort-functions (into [] (drop-last new-group))]
                  (recur (rest groups) (groups.table-unary-op/sort-table table sort-functions desc-vector)))

                (= operator "__project__")
                (let [project-vars (first new-group)
                      project-options (into #{} (second new-group))
                      limit (peek new-group)]
                  (recur (rest groups) (groups.table-unary-op/project-table table project-vars project-options limit true)))

                (= operator "__group-by__")
                (let [[group-by-vars binds filters] new-group]
                  (recur (rest groups) (groups.table-unary-op/group-by-table table group-by-vars binds filters)))

                (= operator "__do-each__")
                (let [f-vec (second (first new-group))]
                  (recur (rest groups) (groups.table-unary-op/run-function table f-vec true)))

                (= operator "__do__")
                (let [f-vec (second (first new-group))]
                  (recur (rest groups) (groups.table-unary-op/run-function table f-vec false)))

                :else (do (println "Unknown operator") (System/exit 0))))))))))


(defn run-group1 [q-info group]
  (loop [groups (split-group group)
         table (fn [] {"sorted-vars" []
                       "table"       {}})]
    (if (empty? groups)
      table
      (let [cur-group (first groups)]
        (if (library.util/basic-pattern? cur-group)
          (recur (rest groups)
                 (partial bgp.bgp-process/process-bgp q-info cur-group))
          (let [new-group (first cur-group)
                operator (str (first new-group))
                new-group (into [] (rest new-group))]
            (if (library.util/binary-op? operator)
              (let [new-table (run-group1 q-info new-group)]
                (cond
                  ;(= operator "__add__") (recur (rest groups) (groups.table-binary-op/union table new-table))
                  (= operator "__add__") (recur (rest groups) (partial groups.table-binary-op/union table new-table))
                  ;(= operator "__if__") (recur (rest groups) (groups.table-binary-op/optional table new-table))
                  ;(= operator "__not__") (recur (rest groups) (groups.table-binary-op/minus table new-table))
                  ;(= operator "__and__") (recur (rest groups) (groups.table-binary-op/join table new-table))
                  (= operator "__and__") (recur (rest groups) (partial groups.table-binary-op/join table new-table))
                  :else (do (println "unknown operator") (recur (rest groups) table))))
              (cond
                (= operator "__project__")
                (let [project-vars (first new-group)
                      project-options (into #{} (second new-group))
                      limit (peek new-group)]
                  ;(recur (rest groups) (groups.table-unary-op/project-table table project-vars project-options limit true))
                  (recur (rest groups) (partial groups.table-unary-op/project-table table project-vars project-options limit true)))


                :else (do (println "Unknown operator") (System/exit 0))))))))))