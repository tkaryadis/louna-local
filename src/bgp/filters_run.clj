(ns bgp.filters-run
  (:require bgp.functions-process))


(defn run-vars-filters-vectors [filters-vector vars values]
  (if-not (empty? filters-vector)
    (let [var-values (zipmap vars values)]
      (reduce (fn [bool-value f-map]
                (let [filter-result (bgp.functions-process/run-f-eval var-values f-map)]
                  (if (and filter-result bool-value)
                    true
                    (reduced false))))
              true
              filters-vector))
    true))


(defn run-table-filter-vectors [var-name table-filter-functions sorted-vars line]
  (if (contains? table-filter-functions var-name)
    (let [var-info (get table-filter-functions var-name)
          dep-vars (get var-info "dep-vars")
          f-maps (get var-info "functions")
          var-values (zipmap sorted-vars line)]
      (reduce (fn [bool-value f-map]
                (let [filter-result (bgp.functions-process/run-f-eval var-values f-map)]
                  (if (and filter-result bool-value)
                    true
                    (reduced false))))
              true
              f-maps))
    true))

;;used only for relations ?r
(defn run-many-1var-filter-vectors [var-name values filters]
  (reduce (fn [values value]
            (let [passed? (bgp.filters-run/run-vars-filters-vectors filters [var-name] [value])]
              (if passed? (conj values value) values)))
          []
          values))