(ns bgp.binds-run
  (:require bgp.functions-process
            library.util
            bgp.filters-run))

(defn add-binds-to-line [sorted-vars binds table-filter-functions line]
  (loop [binds binds
         line line
         sorted-vars sorted-vars]
    (if (or (empty? binds) (empty? line))
      line
      (let [bind (first binds)
            bind-var (get bind "bind-var")
            filters (get bind "filters")
            dep-vars (into [] (get bind "dep-vars"))
            f-map (get bind "f-map")
            dep-vars-values  (reduce (fn [dep-vars-values dep-var]
                                       (let [dep-var-position (library.util/get-var-position sorted-vars dep-var)]
                                         (assoc dep-vars-values dep-var (get line dep-var-position))))
                                     {}
                                     dep-vars)
            bind-value (bgp.functions-process/run-f-eval dep-vars-values f-map)
            bind-line (conj line bind-value)
            bind-sorted-vars (conj sorted-vars bind-var)
            passed? (bgp.filters-run/run-vars-filters-vectors filters [bind-var] [bind-value])
            passed? (if passed?
                      (bgp.filters-run/run-table-filter-vectors bind-var
                                                                table-filter-functions
                                                                bind-sorted-vars
                                                                bind-line)
                      false)
            bind-line (if passed? bind-line [])]
        (recur (rest binds) bind-line bind-sorted-vars)))))