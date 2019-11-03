(ns groups.table-unary-op
  (:require library.util
            clojure.set
            bgp.filters-process
            bgp.filters-run
            bgp.functions-process))

;;--------------------------filter/bind----------------------------------------------

;;Ta filtra ta xorizo se 2 katigories
;;filtra pou mporo na ta efarmoso xoris kanena bind
;;filtra pou mporo na ta efarmoso otan bazo tin sigekrimeni bind-timi

;;arxika stis sorted-vars bazo ta binds

(defn get-bind-filter-line [sorted-vars bind-vars binds-map filters-map line]
  (let [table-filters (get filters-map "table-filters")
        passed? (bgp.filters-run/run-vars-filters-vectors table-filters sorted-vars line)]
    (if passed?
      (loop [bind-vars bind-vars
             line line
             sorted-vars sorted-vars]
        (if (or (empty? bind-vars) (empty? line))
          line
          ;;bazo tin bind-var me tin timi tis stin line
          ;;kai meta trexo ola ta filtra gia autin tin bind-var
          (let [bind-var (first bind-vars)
                bind-f-eval (get binds-map bind-var)
                bind-filters (get-in filters-map ["bind-filters" bind-var])

                vars-values (zipmap sorted-vars line)
                bind-value (bgp.functions-process/run-f-eval vars-values bind-f-eval)
                bind-line (conj line bind-value)
                sorted-vars-bind (conj sorted-vars bind-var)
                passed? (bgp.filters-run/run-vars-filters-vectors bind-filters sorted-vars-bind bind-line)
                new-line (if passed? bind-line [])]
            (recur (rest bind-vars) new-line sorted-vars-bind))))
      [])))

;;filters-map {"table-filters" [f1 f2...] "bind-filters" {"bind-var1" [f1 f2 f3 ...]}}
(defn get-filters-map [sorted-vars sortedbind-vars filter-queries]
  (let [sorted-vars-set (into #{} sorted-vars)]
    (loop [filter-queries filter-queries
           filters-map {"table-filters" [] "bind-filters" {}}]
      (if (empty? filter-queries)
        filters-map
        (let [filter-query (first filter-queries)
              [filter-var dep-vars filter-function] (bgp.functions-process/table-filter-query-to-function
                                                      sortedbind-vars
                                                      filter-query)
              filter-function (bgp.functions-process/vector-to-f-eval filter-function)
              ]
          (if (contains? sorted-vars-set filter-var)
            (recur (rest filter-queries) (assoc filters-map "table-filters"
                                                            (conj (get filters-map "table-filters") filter-function)))
            (let [prv-bind-map (get filters-map "bind-filters")
                  prv-bind-filters (if (contains? prv-bind-map filter-var)
                                     (get prv-bind-map filter-var)
                                     [])
                  new-bind-filters (conj prv-bind-filters filter-function)]
              (recur (rest filter-queries) (assoc-in filters-map ["bind-filters" filter-var] new-bind-filters)))))))))


(defn get-bind-map [bind-vars bind-queries]
  (let [bind-code-list (map (fn [query] (second query)) bind-queries)
        bind-vector-list (map (comp bgp.functions-process/vector-to-f-eval
                                bgp.functions-process/get-bind-function)
                              bind-code-list)]
    (zipmap bind-vars bind-vector-list)))

(defn filter-bind [table-info bind-queries filter-queries]
  (let [sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        bind-vars (into [] (map first bind-queries))
        binds-map (get-bind-map bind-vars bind-queries)
        sortedbind-vars (into [] (concat sorted-vars bind-vars))
        filters-map (get-filters-map sorted-vars sortedbind-vars filter-queries)
        line-keys (keys table)]
    (loop [line-keys line-keys
           index 0
           final-table {}]
      (if (empty? line-keys)
        {"sorted-vars" sortedbind-vars
         "table" final-table}
        (let [line (get table (first line-keys))
              new-line (get-bind-filter-line sorted-vars bind-vars binds-map filters-map line)]
          (if (empty? new-line)
            (recur (rest line-keys) index final-table)
            (recur (rest line-keys) (inc index) (assoc final-table index new-line))))))))



;;--------------------------------sort-by------------------------------------------------------

(defn compare-lines [sorted-vars line1 line2 sort-functions desc-vector]
  (loop [sort-functions sort-functions
         desc-vector desc-vector]
    (if (empty? sort-functions)          ;;adiasa xoris na exo epistrepsi ara ises lines
      (compare 0 0)
      (let [cur-sort-function (first sort-functions)]
        (if (= (count cur-sort-function) 2)
          (let [f-eval (get cur-sort-function 0)
                dep-indexes (get cur-sort-function 1)
                r #(reduce (fn [var-values dep-index]
                             (assoc var-values (get sorted-vars dep-indexes) (get % dep-index)))
                           {}
                           dep-indexes)
                vars-values1 (r line1)
                vars-values2 (r line2)
                ;vars-values1 (zipmap sorted-vars line1)
                ;vars-values2 (zipmap sorted-vars line2)
                line1-value (bgp.functions-process/run-f-eval vars-values1 f-eval)
                line2-value (bgp.functions-process/run-f-eval vars-values2 f-eval)]
            (if (not= line1-value line2-value)
              (if (= (first desc-vector) :asc)
                (compare line1-value line2-value)
                (compare line2-value line1-value))
              (recur (rest sort-functions) (rest desc-vector))))
          (let [line1-value (get line1 (first cur-sort-function))
                line2-value (get line2 (first cur-sort-function))]
            (if (not= line1-value line2-value)
              (if (= (first desc-vector) :asc)
                (compare line1-value line2-value)
                (compare line2-value line1-value))
              (recur (rest sort-functions) (rest desc-vector)))))))))

(defn sort-table-lines [table sorted-vars sort-functions desc-vector]
  (into (sorted-map-by (fn [key1 key2]
                         (let [line1 (get table key1)
                               line2 (get table key2)
                               result (compare-lines sorted-vars line1 line2 sort-functions desc-vector)
                               result (if (= result 0) (compare key1 key2) result)]
                           result)))
        table))

(defn sort-table [table-info sort-functions desc-vector]
  (let [sorted-vars (get table-info "sorted-vars")
        sort-functions (map (fn [arg]
                              (if (clojure.string/starts-with? arg "(")
                                (let [[dep-vars f-vector] (bgp.functions-process/get-vars-filter-function arg)
                                       dep-vars-indexes (map (partial library.util/get-var-position sorted-vars) dep-vars)
                                       f-eval (bgp.functions-process/vector-to-f-eval f-vector)]
                                  [f-eval dep-vars-indexes])
                                  [(library.util/get-var-position sorted-vars arg)]))
                            sort-functions)
        table (get table-info "table")
        sorted-table (sort-table-lines table sorted-vars sort-functions desc-vector)]
    {"sorted-vars" sorted-vars
     "sorted?" true
     "table" sorted-table}))


;;---------------------------------project-----------------------------------------------------

(defn get-distinct-table [sorted? table]
  (if-not sorted?
    (let [table-lines (distinct (vals table))]
      (zipmap (take (count table) (range (count table))) table-lines))
    (loop [lines (vals table)
           d-table (sorted-map)
           prv-line []]
      (if (empty? lines)
        d-table
        (let [line (first lines)]
          (if (= line prv-line)
            (recur (rest lines) d-table prv-line)
            (recur (rest lines) (assoc d-table (state.state/get-new-line-ID) line) line)))))))

(declare project-table)

(defn limit-table [table-info limit]
  (if (= limit -1)
    table-info
    (let [sorted-vars (get table-info "sorted-vars")
          table (get table-info "table")
          sorted? (get table-info "sorted?" false)
          limited-table (reduce (fn [limited-table line]
                                  (assoc limited-table (state.state/get-new-line-ID) line))
                                (if sorted? (sorted-map) {})
                                (take limit (vals table)))]
      {"sorted-vars" sorted-vars
       "sorted?" sorted?
       "table" limited-table})))

(defn remove-temp-vars [table-info project-options limit]
  (let [sorted-vars (get table-info "sorted-vars")
        del-vars (into #{} (filter (fn [v] (or (clojure.string/starts-with? v "TMP")
                                               ;(clojure.string/starts-with? v "TTMP")
                                               )) sorted-vars))
        keep-vars (into [] (filter #(not (contains? del-vars %)) sorted-vars))]
    (if (empty? del-vars)
      (if (= limit -1)
        table-info
        (limit-table table-info limit))
      (project-table table-info keep-vars #{} limit false))))

;;ean exo project vars tote kano apla project
;;ean exo :all bgazo mono tis tmp
(defn project-table [table-info project-vars project-options limit check-temp?]
  (let [sorted-vars (get table-info "sorted-vars")
        - (if-not (clojure.set/subset? (into #{} project-vars) (into #{} sorted-vars))
            (do (prn "Project-vars-not-found" (clojure.set/difference (into #{} project-vars)
                                                                      (into #{} sorted-vars)))
                (System/exit 0)))]
    (if (and (= project-options #{:all}) check-temp?)
      (remove-temp-vars table-info project-options limit)
      (let [sorted? (get table-info "sorted?" false)
            table (get table-info "table")
            project-vars (if (contains? project-options :all) sorted-vars project-vars)

            distinct-vars? (contains? project-options :distinct)
            line-indexes (map (partial library.util/get-var-position sorted-vars) project-vars)
            project-table (reduce (fn [project-table line]
                                    (let [project-line (reduce (fn [p-line cur-index]
                                                                 (conj p-line (get line cur-index)))
                                                               []
                                                               line-indexes)]
                                      (assoc project-table (count project-table) project-line)))
                                  (if sorted? (sorted-map) {})
                                  (if (= limit -1) (vals table) (take limit (vals table))))
            project-table (if distinct-vars? (get-distinct-table sorted? project-table) project-table)]
        {"sorted-vars" project-vars
         "sorted?" sorted?
         "table" project-table}))))

;;---------------------------------group-by------------------------------------------------------

;;ean balo kai deuteri metabliti simeni oti mikreno akoma pio poli ta groups
;;stin idia omada anikoun 2 rows ean exoun idies times kai stin dio metablites

;;px group by ?country ?city


;;group by ?var simeni oses rows exoun idia timi se auti tin var = group
;;ara xorizo ton table se omades pou exoun idia timi stin var auti

;;meta efarmozo aggr-bind sto kathe group
;;diladi to kathe group ginete telika 1 grami ston pinaka(stiles i metabliti tou group + oses kano aggr-bind)

;;meta efarmozo filtra


(defn add-aggr-values-to-line [agr-sorted-vars line bind-vars vars-aggr-values filters-map]
  (loop [bind-vars bind-vars
         line line
         passed? true]
    (if (or (not passed?) (empty? bind-vars)) ;;ite ebala ola ta binds stin line,ite line kopike apo filtro
      line
      (let [bind-var (first bind-vars)
            agr-value (get vars-aggr-values bind-var)
            filters (get filters-map bind-var)
            new-line (conj line agr-value)
            passed? (bgp.filters-run/run-vars-filters-vectors filters agr-sorted-vars new-line)
            new-line (if passed? new-line [])]
        (recur (rest bind-vars) new-line passed?)))))

(defn add-from-line-to-col [line vars-indexes-map vars-col]
  (loop [vars (keys vars-col)
         vars-col vars-col]
    (if (empty? vars)
      vars-col
      (let [cur-var (first vars)
            cur-var-index (get vars-indexes-map cur-var)
            line-value (get line cur-var-index)
            prv-value (get vars-col cur-var)
            new-value (conj prv-value line-value)]
        (recur (rest vars) (assoc vars-col cur-var new-value))))))

(defn get-qvar-col [lines vars-indexes-map]
  (loop [lines lines
         vars-col (zipmap (keys vars-indexes-map) (take (count vars-indexes-map) (repeat [])))]
    (if (empty? lines)
      vars-col
      (let [line (first lines)]
        (recur (rest lines) (add-from-line-to-col line vars-indexes-map vars-col))))))

;;run bind-vectors
;;arxika exo var:bind-vector
;;telika tha exo var:bind-timi
(defn run-bind-vectors [group-lines dep-vars-indexes vars-bind-vectors]
  (let [var-values (get-qvar-col group-lines dep-vars-indexes)]
    (loop [bind-vars (keys vars-bind-vectors)
           vars-bind-vectors vars-bind-vectors]
      (if (empty? bind-vars)
        vars-bind-vectors
        (let [bind-var (first bind-vars)
              f-eval (get vars-bind-vectors bind-var)
              agr-value (bgp.functions-process/run-f-eval var-values f-eval)]
          (recur (rest bind-vars) (assoc vars-bind-vectors bind-var agr-value)))))))

;;gia kathe timi tou pinaka(ean mia group-by metabliti exo 1 stili ston pinaka)
;;brisko to group tou
;;efarmozo ola ta binds me tin seira pou dothikan
;;kathe fora pou bazo kapio bind protou balo to epomeno trexo ola ta filtra tou
(defn add-binds-to-table [agr-sorted-vars agr-table groups bind-vars dep-vars-indexes vars-bind-vectors filters-map]
  (loop [line-keys (keys agr-table)
         agr-table agr-table]
    (if (empty? line-keys)
      agr-table
      (let [line-key (first line-keys)
            line (get agr-table line-key)
            group-lines (get groups line)
            line (if (= line ["*"]) [] line)
            vars-aggr-values (run-bind-vectors group-lines dep-vars-indexes vars-bind-vectors)
            new-line (add-aggr-values-to-line agr-sorted-vars line bind-vars vars-aggr-values filters-map)]
        (if (empty? new-line)
          (recur (rest line-keys) (dissoc agr-table line-key))
          (recur (rest line-keys) (assoc agr-table line-key new-line)))))))

(defn get-dep-vars [code]
  (map (fn [cur-var] (subs cur-var 1)) (re-seq #"\?\w+" (str code))))


(defn get-having-filters-map [agr-sorted-vars filters]
  (loop [filters filters
         filters-map {}]
    (if (empty? filters)
      filters-map
      (let [filter-code (first filters)
            vars (get-dep-vars filter-code)
            vars-indexes (map (partial library.util/get-var-position agr-sorted-vars) vars)
            max-index (apply max vars-indexes)
            max-var (get agr-sorted-vars max-index)
            [- - filter-vector] (bgp.functions-process/table-filter-query-to-function agr-sorted-vars filter-code)
            filter-vector (bgp.functions-process/vector-to-f-eval filter-vector)]
        (if (contains? filters-map max-var)
          (let [prv-value (get filters-map max-var)
                new-value (conj prv-value filter-vector)]
            (recur (rest filters) (assoc filters-map max-var new-value)))
          (recur (rest filters) (assoc filters-map max-var [filter-vector])))))))

(defn group-by-table [table-info group-vars binds filters]
  (let [;;kathe filtro tha exi simfona me tis new-sorted-vars
        ;;to simio pou tha efarmosti
        sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        lines (vals table)

        group-vars-indexes (map-indexed (fn [index value] index) group-vars)
        group-vars-indexes-set (into #{} group-vars-indexes)
        bind-vars (map (comp library.util/get-var-name str first) binds)
        agr-sorted-vars (if (empty? group-vars)
                          (into [] bind-vars)
                          (into [] (concat group-vars bind-vars)))

        ;;the variables from the initial table that all the binds will use
        binds-dep-vars (distinct (reduce (fn [dep-vars bind-query]
                                           (let [bind-code (second bind-query)
                                                 bind-code-vars (get-dep-vars bind-code)]
                                             (apply (partial conj dep-vars) bind-code-vars)))
                                         []
                                         binds))
        binds-dep-vars-indexes (map (partial library.util/get-var-position sorted-vars) binds-dep-vars)
        dep-vars-indexes (zipmap binds-dep-vars binds-dep-vars-indexes)
        bind-vectors (map (comp bgp.functions-process/get-bind-function second) binds)
        bind-vectors (map bgp.functions-process/vector-to-f-eval bind-vectors)
        vars-bind-vectors (zipmap bind-vars bind-vectors)

        groups (if (empty? group-vars)
                 {["*"] (vals table)}
                 (group-by (fn [line]
                             (let [key-line (second
                                              (reduce
                                                (fn [[index subline] line-value]
                                                  (let [index-subline (library.util/get-index-sorted-vars
                                                                       group-vars sorted-vars index)]
                                                    (if (contains? group-vars-indexes-set index-subline)
                                                      [(inc index) (assoc subline index-subline line-value)]
                                                      [(inc index) subline])))
                                                [0 (into [] (range 0 (count group-vars)))]
                                                line))]
                               key-line)
                           )
                           lines))

        ;;arxika o aggr-table exi mono mia stili tin qvar me tis distinct times
        ;;tou bazo ena ena ta binds kai se kathe bind pou bazo elenxo to filter tou(ean exi)
        ;;ean pernai to bazo
        distinct-values (keys groups)
        filters-map (get-having-filters-map agr-sorted-vars filters)
        agr-table (zipmap (range (count distinct-values)) distinct-values)
        agr-table (if-not (empty? binds)
                    (add-binds-to-table agr-sorted-vars
                                        agr-table
                                        groups
                                        bind-vars
                                        dep-vars-indexes
                                        vars-bind-vectors
                                        filters-map)
                    agr-table)]
    {"sorted-vars" agr-sorted-vars
     "table" agr-table}))


;;-------------------------------do--------------------------------------------------------------

(defn run-vars-function [table-info f-eval table?]
  (let [sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")]
    (loop [lines-keys (keys table)]
      (if (empty? lines-keys)
        nil
        (let [line-key (first lines-keys)
              line (get table line-key)
              vars-values (zipmap sorted-vars line)
              vars-values (if table?
                            (assoc vars-values "qtable" table-info)
                            vars-values)
              - (bgp.functions-process/run-f-eval vars-values f-eval)]
          (recur (rest lines-keys)))))))

(defn run-aggr-function [table-info dep-vars f-eval table?]
  (let [sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        dep-vars-indexes (map (partial library.util/get-var-position sorted-vars) dep-vars)
        dep-vars-indexes (zipmap dep-vars dep-vars-indexes)
        lines (vals table)
        vars-values (get-qvar-col lines dep-vars-indexes)
        vars-values (if table?
                      (assoc vars-values "qtable" table-info)
                      vars-values)
        - (bgp.functions-process/run-f-eval vars-values f-eval)]))

(defn run-function [table-info function-str do?]
  (let [[dep-vars-set f-vector] (bgp.functions-process/get-vars-filter-function function-str)
        table? (contains? dep-vars-set "qtable")
        dep-vars (if table?
                   (into [] (disj dep-vars-set "qtable"))
                   (into [] dep-vars-set))
        f-eval (bgp.functions-process/vector-to-f-eval f-vector)]
    (if do?
      (do (run-vars-function table-info f-eval table?)
          table-info)
      (do (run-aggr-function table-info dep-vars f-eval table?)
          table-info))))



;;--------------------------------Query table+binds+filters-----------------------------------------------

(defn is-line-vec? [line]
  (= (subs line 0 1) "["))

(defn line-to-vec [line]
  (let [line-parts (clojure.string/split line #"\s+")
        line (into [] (map library.util/get-typed-token line-parts))]
    line))

(defn read-table [table-name sorted-vars bind-queries filter-queries]
  (let [indexes-ignore (into #{} (filter #(> % 0) (map-indexed (fn [index value] (if (= value "-") index -1))
                                                               sorted-vars)))
        sorted-vars (into [] (filter #(not (= % "-")) sorted-vars))
        bind-vars (into [] (map first bind-queries))
        binds-map (get-bind-map bind-vars bind-queries)
        sortedbind-vars (into [] (concat sorted-vars bind-vars))
        filters-map (get-filters-map sorted-vars sortedbind-vars filter-queries)]
    (with-open [rdr (clojure.java.io/reader (str (state.db-settings/get-tables-path) table-name))]
      (loop [lines (line-seq rdr)
             index 0
             table {}
             line-vec? false]
        (let [line (first lines)
              line-vec? (if (= index 0) (is-line-vec? line) line-vec?)]
          (if (or (empty? lines) (clojure.string/blank? line))
            {"sorted-vars"  sortedbind-vars
             "table" table}
            (let [line (if line-vec? (read-string line) (line-to-vec line))]
              (if (empty? indexes-ignore)
                (let [line-bind-filter (get-bind-filter-line sorted-vars
                                                             bind-vars
                                                             binds-map
                                                             filters-map
                                                             line)]
                  (if-not (empty? line-bind-filter)
                    (recur (rest lines) (inc index) (assoc table index line-bind-filter) line-vec?)
                    (recur (rest lines) (inc index) table line-vec?)))
                (let [[- line] (reduce (fn [[index row-values] value]
                                                   (if (contains? indexes-ignore index)
                                                     [(inc index) row-values]
                                                     [(inc index) (conj row-values value)]))
                                                 [0 []]
                                                 line)
                      line-bind-filter (get-bind-filter-line sorted-vars
                                                             bind-vars
                                                             binds-map
                                                             filters-map
                                                             line)]
                  (if-not (empty? line-bind-filter)
                    (recur (rest lines) (inc index) (assoc table index line-bind-filter) line-vec?)
                    (recur (rest lines) (inc index) table line-vec?)))))))))))