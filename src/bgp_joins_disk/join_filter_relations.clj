(ns bgp-joins-disk.join-filter-relations
  (:require library.util
            bgp.filters-run
            bgp-joins-disk.relations-run-filter
            bgp-joins-disk.path-run-seq-star-plus))

(defn get-join-table-filter-functions [table-filter-functions binds]
  (loop [binds binds
         join-table-filter-functions table-filter-functions]
    (if (empty? binds)
      join-table-filter-functions
      (let [bind (first binds)
            bind-var (get bind "bind-var")]
        (recur (rest binds) (assoc join-table-filter-functions bind-var (get table-filter-functions bind-var [])))))))

(defn get-filter-set [q-id sorted-vars relation]
  (let [rel-type (get relation "rel-type")
        property (get relation "property")]
    (cond
      (= rel-type "s")
      ["s"
       (library.util/get-var-position sorted-vars (get relation "s-var"))
       (cond
         (nil? property)
         (bgp-joins-disk.relations-run-filter/triple-filter-s q-id relation)
         (= (get property "prop-type") "alt")
         (bgp-joins-disk.relations-run-filter/triple-filter-rs q-id relation true)
         (= (get property "prop-type") "seq+")
         (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-id relation nil true false)
         (= (get property "prop-type") "seq*")
         (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-id relation nil true true))]
      (= rel-type "o")
      ["o"
       (library.util/get-var-position sorted-vars (get relation "o-var"))
       (cond
         (nil? property)
         (bgp-joins-disk.relations-run-filter/triple-filter-o q-id relation)
         (= (get property "prop-type") "alt")
         (bgp-joins-disk.relations-run-filter/triple-filter-ro q-id relation true)
         (= (get property "prop-type") "seq+")
         (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-id relation nil false false)
         (= (get property "prop-type") "seq*")
         (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-id relation nil false true))]
      (= rel-type "so")
      ["so"
       (library.util/get-var-position sorted-vars (first (get relation "pair")))
       (library.util/get-var-position sorted-vars (second (get relation "pair")))
       (cond
         (nil? property)
         (bgp-joins-disk.relations-run-filter/triple-filter-so q-id relation)
         (= (get property "prop-type") "alt")
         (bgp-joins-disk.relations-run-filter/triple-filter-rso q-id relation true)
         (= (get property "prop-type") "seq+")
         (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-id relation 1 nil false)
         (= (get property "prop-type") "seq*")
         (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-id relation 1 nil true))]
      (= rel-type "r")
      ["r"
       (library.util/get-var-position sorted-vars (first (get relation "triple")))
       (bgp-joins-disk.relations-run-filter/triple-filter-r q-id relation)]
      (= rel-type "rs")
      ["rs"
       (library.util/get-var-position sorted-vars (first (get relation "triple")))
       (library.util/get-var-position sorted-vars (second (get relation "triple")))
       (bgp-joins-disk.relations-run-filter/triple-filter-rs q-id relation false)]
      (= rel-type "ro")
      ["ro"
       (library.util/get-var-position sorted-vars (first (get relation "triple")))
       (library.util/get-var-position sorted-vars (second (get relation "triple")))
       (bgp-joins-disk.relations-run-filter/triple-filter-ro q-id relation false)]
      (= rel-type "rso")
      ["rso"
       (library.util/get-var-position sorted-vars (first (get relation "triple")))
       (library.util/get-var-position sorted-vars (second (get relation "triple")))
       (library.util/get-var-position sorted-vars (nth (get relation "triple") 2))
       (bgp-joins-disk.relations-run-filter/triple-filter-rso q-id relation false)])))


(defn get-filter-relation-function [filter-relation]
  (partial (cond
             (= (get filter-relation 0) "s")
             (fn [filter-relation line] (let [s-index (get filter-relation 1)
                                              filter-set (get filter-relation 2)
                                              line-value (get line s-index)]
                                          (contains? filter-set line-value)))
             (= (get filter-relation 0) "o")
             (fn [filter-relation line] (let [o-index (get filter-relation 1)
                                              filter-set (get filter-relation 2)
                                              line-value (get line o-index)]
                                          (contains? filter-set line-value)))
             (= (get filter-relation 0) "so")
             (fn [filter-relation line] (let [s-index (get filter-relation 1)
                                              o-index (get filter-relation 2)
                                              filter-set (get filter-relation 3)
                                              line-value [(get line s-index) (get line o-index)]]
                                          (contains? filter-set line-value)))
             (= (get filter-relation 0) "r")
             (fn [filter-relation line] (let [r-index (get filter-relation 1)
                                              filter-set (get filter-relation 2)]
                                          (contains? filter-set (get line r-index))))
             (= (get filter-relation 0) "rs")
             (fn [filter-relation line] (let [r-index (get filter-relation 1)
                                              o-index (get filter-relation 2)
                                              filter-set (get filter-relation 3)
                                              line-value [(get line r-index) (get line o-index)]]
                                          (contains? filter-set line-value)))
             (= (get filter-relation 0) "ro")
             (fn [filter-relation line] (let [r-index (get filter-relation 1)
                                              o-index (get filter-relation 2)
                                              filter-set (get filter-relation 3)
                                              line-value [(get line r-index) (get line o-index)]]
                                          (contains? filter-set line-value)))
             (= (get filter-relation 0) "rso")
             (fn [filter-relation line] (let [r-index (get filter-relation 1)
                                              s-index (get filter-relation 2)
                                              o-index (get filter-relation 3)
                                              filter-set (get filter-relation 4)
                                              line-value [(get line s-index)
                                                          (get line o-index)
                                                          (get line r-index)]]
                                          (contains? filter-set line-value))))
           filter-relation))

(defn apply-filter-relations [q-id sorted-vars table filter-relations]
  (if-not (empty? filter-relations)
    (let [filter-relations (map (partial get-filter-set q-id sorted-vars) filter-relations)
          filter-relations-functions (map get-filter-relation-function filter-relations)]
      (loop [table-keys (keys table)
             table table]
        (if (empty? table-keys)
          table
          (let [cur-key (first table-keys)
                line (get table cur-key)
                passed? (reduce #(and %1 %2) true (map (fn [f] (f line)) filter-relations-functions))
                new-table (if passed? table (dissoc table cur-key))]
            (recur (rest table-keys) new-table)))))
    table))




(comment
  ;;sxesi me metablites pou idi iparxoun ston pinaka
  ;;autes kanonika den tis trexo mones tous,ti trexo san filtra otan pao na kano ena join me add-var
  (defn filter-line-2vars [line s-var-index o-var-index relation-map]
    (let [s-line-value (get line s-var-index)
          o-line-value (get line o-var-index)]
      (if (contains? relation-map s-line-value)
        (let [o-relation-value (get relation-map s-line-value)]
          (if (coll? o-relation-value)
            (if (contains? (into #{} o-relation-value) o-line-value)
              line
              [])
            (if (= o-relation-value o-line-value)
              line
              [])))
        [])))

  ;;autes kanonika den tis trexo mones tous,ti trexo san filtra otan pao na kano ena join me add-var
  (defn filter-line-1var [line join-var-index relation-set]
    (let [line-value (get line join-var-index)]
      (if (contains? relation-set line-value)
        line
        []))))