(ns bgp-joins.joins-run
  (:require bgp-joins.relations-run
            bgp-joins.relations-run-filter
            state.state))

(defn filter-relations-added-to-relations [relations]
  (loop [relations relations
         new-relations []]
    (if (empty? relations)
      new-relations
      (let [relation (first relations)
            filter-relations (get relation "filter-relations")
            filter-relations (map (fn [fr]
                                    (assoc fr "filter" true))
                                  filter-relations)
            new-relations (apply (partial conj new-relations) (concat (list (dissoc relation "filter-relations")) filter-relations))]
        (recur (rest relations) new-relations)))))

(defn make-joins [q-info table-info]
  (let [relations (get table-info "relations")
        relations (filter-relations-added-to-relations relations)
        final-sorted-vars (get table-info "final-sorted-vars")
        final-sorted-vars-no-temp (into [] (filter #(not (clojure.string/starts-with? % "TTMP")) final-sorted-vars))
        table-filter-functions (get table-info "table-filter-functions")]
    (loop [relations relations
           sorted-vars []
           hash-table {}]
      (if (empty? relations)
        {"sorted-vars" sorted-vars
         "table" hash-table}
        (let [cur-relation (first relations)

              ;;ttemp var can't be a join var,so join-vars and next-join vars arent effected
              cur-join-vars (library.util/sort-vars final-sorted-vars (get cur-relation "join-vars" []))
              cur-add-vars (library.util/sort-vars final-sorted-vars (get cur-relation "add-vars" []))
              last-join? (= (count relations) 1)
              next-join-vars (if-not  last-join?
                               (library.util/sort-vars final-sorted-vars (get (second relations) "join-vars" []))
                               [])
              next-join-vars-indexes (if-not last-join?
                                       (into [] (map (partial library.util/get-var-position final-sorted-vars-no-temp)
                                                     next-join-vars))
                                       [])

              join-relation? (not (get cur-relation "filter" false))]

          (if join-relation?
            (let [new-sorted-vars (into [] (concat sorted-vars
                                                   (filter #(not (library.util/ttmp-var? %)) cur-add-vars)
                                                   (map (fn [b] (get b "bind-var")) (get cur-relation "binds"))))
                  hash-table (bgp-joins.relations-run/triple-join-rso q-info
                                                                      cur-relation
                                                                      sorted-vars
                                                                      cur-add-vars
                                                                      hash-table
                                                                      cur-join-vars
                                                                      next-join-vars-indexes
                                                                      table-filter-functions)]
              (recur (rest relations) new-sorted-vars hash-table))
            (let [hash-table (bgp-joins.relations-run-filter/triple-filter-rso
                               q-info
                               cur-relation
                               sorted-vars
                               hash-table
                               cur-join-vars
                               next-join-vars-indexes)]
              (recur (rest relations) sorted-vars hash-table))))))))