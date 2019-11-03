(ns library.print-data
  (:require state.state))

;;------------------------------print-table------------------------------------

(defn print-table-stdout [table-info]
  (let [sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        - (println sorted-vars)
        - (dorun (map (partial apply prn) (vals table)))
        - (println)]
    table-info))

(defn print-table-to-file [sorted-vars table path]
  (with-open [wrtr (clojure.java.io/writer (str (state.db-settings/get-tables-path) path))]
    (loop [lines (vals table)]
      (let [line (str (first lines))]
        (if (empty? lines)
          nil
          (do (.write wrtr (str line "\n"))
              (recur (rest lines))))))))

;;---------------------query-plan-----------------------------------
(defn print-filters [filters spaces]
  (let [filters-vals (apply concat (vals filters))]
    (doall (map (fn [filter] (println spaces (get filter "query"))) filters-vals))))

(defn print-bind-filters [filters]
  (doall (map (fn [filter] (println "    " (get filter "query"))) filters)))

(defn print-binds [relation-binds filters?]
  (dorun (map (fn [bind]
                (let [filters (get bind "filters")]
                  (do (println "    bind:" "(" (get-in bind ["f-map" "query"]) (str "?" (get bind "bind-var")) ")")
                      (if filters? (print-bind-filters filters)))))
              relation-binds)))

(defn print-filter-relations [filter-relations filters?]
  (dorun (map (fn [relation]
                (let [filters (get relation "filters")]
                  (do (println "    relation(filter):" (get relation "query")
                               "filter-vars" (into [] (get relation "vars")))
                      (if filters? (print-filters filters "    ")))))
              filter-relations)))

(defn print-query-plan [relations filters?]
  (println "Query plan")
  (dorun (map (fn [relation]
                (let [filters (get relation "filters")
                      relation-binds (get relation "binds" [])
                      filter-relations (get relation "filter-relations" [])
                      - (do (println "relation:" (get relation "query")
                                     "join-vars:" (into [] (get relation "join-vars"))
                                     "add-vars:" (into [] (get relation "add-vars"))))
                      - (if filters? (print-filters filters " "))
                      - (print-binds relation-binds filters?)
                      - (print-filter-relations filter-relations filters?)]))
              relations))
  (prn))

;;-------------------print-constructs-------------------------------------


(defn print-constructs [constructs]
  (dorun (map (fn [relation]
                (let [construct (get constructs relation)
                      - (prn relation)
                      - (dorun (map prn construct))
                      - (prn)]))
              (keys (dissoc constructs :size)))))
