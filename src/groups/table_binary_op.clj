(ns groups.table-binary-op
  (:require library.util
            clojure.set))

;;-----------------------------------shared-functions--------------------------------------------------
;;-----------------------------add-lines-to-table------------------------------------------------------

(defn concat-lines [line1 line2]
  (into [] (concat line1 line2)))

;;oi lines autes einai lines pou kanoun join stin line
;;ara tis kolao stin line kai tis bazo ston table
(defn add-lines-table [table line common-vars-indexes lines]
  (loop [lines lines
         table table]
    (if (empty? lines)
      table
      (recur (rest lines)
             (assoc table
               (state.state/get-new-line-ID)
               (concat-lines line
                             (library.util/get-not-indexes-subline common-vars-indexes
                                                                   (first lines))))))))

(defn add-lines-table-change-order [table line common-vars-indexes lines joined-vars final-sorted-vars]
  (loop [lines lines
         table table]
    (if (empty? lines)
      table
      (let [line (concat-lines line
                               (library.util/get-not-indexes-subline common-vars-indexes
                                                                     (first lines)))
            line (library.util/sort-line-by-sorted-vars final-sorted-vars joined-vars line)]
        (recur (rest lines)
               (assoc table
                 (state.state/get-new-line-ID)
                 line))))))

;;----------------------add-nils-----------------------------------------------------------------------
(defn add-nils [table1 sorted-vars1 table2 sorted-vars2 final-table right]
  (loop [lines-keys (if right (keys table1) (keys table2))
         line-index (count final-table)
         final-table final-table]
    (if (empty? lines-keys)
      final-table
      (recur (rest lines-keys)
             (inc line-index)
             (assoc final-table
               line-index
               (if right
                 (concat-lines (get table1 (first lines-keys))
                               (take (count sorted-vars2) (repeat nil)))
                 (concat-lines (take (count sorted-vars1) (repeat nil))
                               (get table2 (first lines-keys)))))))))

;;-----------------------------table-to-hash-table-----------------------------------------------------
(defn get-table-hash [var-indexes table]
  (loop [lines-keys (keys table)
         table-hash {}]
    (if (empty? lines-keys)
      table-hash
      (let [line (get table (first lines-keys))
            subline (library.util/get-indexes-subline var-indexes line)]
        (if (contains? table-hash subline)
          (recur (rest lines-keys) (update table-hash subline conj line))
          (recur (rest lines-keys) (assoc table-hash subline [line])))))))

;;it also stores the line-keys of the table for each group,used in MINUS
(defn get-table-hash-line-keys [var-indexes table]
  (loop [lines-keys (keys table)
         table-hash {}]
    (if (empty? lines-keys)
      table-hash
      (let [line (get table (first lines-keys))
            subline (library.util/get-indexes-subline var-indexes line)]
        (if (contains? table-hash subline)
          (let [prv-value (get table-hash subline)
                prv-lines-keys (get prv-value "lines-keys")
                prv-lines (get prv-value "lines")
                new-value (assoc prv-value "lines-keys" (conj prv-lines-keys (first lines-keys))
                                           "lines" (conj prv-lines line))]
            (recur (rest lines-keys) (assoc table-hash subline new-value)))
          (recur (rest lines-keys) (assoc table-hash subline {"lines-keys" [(first lines-keys)] "lines" [line]})))))))

;;-----------------------------------UNION-------------------------------------------------------------
;;union simeni enose ta ,alla xoris na kanis joins stis koines metablites
;;teliko head set ta dio sorted-vars,kathe grami aniki ston 1 pinaka XOR ston alo

;;UNION when no common variables
(defn no-commons-union [sorted-vars1 sorted-vars2 table1 table2]
  (let [line1-size (count sorted-vars1)
        line2-size (count sorted-vars2)
        final-table-right (add-nils table1 sorted-vars1 table2 sorted-vars2 {} true)
        final-table-left (add-nils table1 sorted-vars1 table2 sorted-vars1 final-table-right false)]
    [(into [] (concat sorted-vars1 sorted-vars2)) final-table-left]))

(defn add-line [sorted-vars sorted-vars1 line1]
  (let [line (into [] (take (count sorted-vars) (repeat nil)))
        line1-map (zipmap sorted-vars1 line1)]
    (first (reduce (fn [[line index] var-name]
                     (if (contains? line1-map var-name)
                       [(assoc line index (get line1-map var-name)) (inc index)]
                       [line (inc index)]))
                   [line 0]
                   sorted-vars))))

(defn add-table-to-union [sorted-vars table sorted-vars1 table1]
  (loop [lines-keys (keys table1)
         index (count table)
         table table]
    (if (empty? lines-keys)
      table
      (let [new-line (add-line sorted-vars sorted-vars1 (get table1 (first lines-keys)))]
        (recur (rest lines-keys) (inc index) (assoc table index new-line))))))

;;ftiaxno ena pinaka me ena megalo head,kai apla pernao tous pinakes kai bazo times
;;sta antistixa collumns,kai nil sta ipolipa
(defn commons-union [sorted-vars1 sorted-vars2 table1 table2]
  (let [sorted-vars (into [] (into #{} (concat sorted-vars1 sorted-vars2)))
        table (add-table-to-union sorted-vars {} sorted-vars1 table1)
        table (add-table-to-union sorted-vars table sorted-vars2 table2)]
    [sorted-vars table]))

(defn empty-vars-union-join [sorted-vars1 sorted-vars2 table1 table2]
  (cond
    (and (empty? sorted-vars2) (empty? sorted-vars1))
    {"sorted-vars" []
     "table" {}}
    (empty? sorted-vars1)
    {"sorted-vars" sorted-vars2
     "table" table2}
    (empty? sorted-vars2)
    {"sorted-vars" sorted-vars1
     "table" table1}
    :else {}))

(defn union [table-info1 table-info2]
  (let [sorted-vars1 (get table-info1 "sorted-vars")
        sorted-vars2 (get table-info2 "sorted-vars")
        table1 (get table-info1 "table")
        table2 (get table-info2 "table")
        common-vars (clojure.set/intersection (into #{} sorted-vars1) (into #{} sorted-vars2))
        ]
    (if (empty? common-vars)
      (let [empty-result (empty-vars-union-join sorted-vars1 sorted-vars2 table1 table2)]
        (if-not (empty? empty-result)
          empty-result
          (let [[sorted-vars table] (no-commons-union sorted-vars1 sorted-vars2 table1 table2)]
            {"sorted-vars" sorted-vars
             "table" table})))
      (let [[sorted-vars table] (commons-union sorted-vars1 sorted-vars2 table1 table2)]
        {"sorted-vars" sorted-vars
         "table" table}))))


;;--------------------------------------MINUS-------------------------------------------------

;;minus simeni otan join petao grami apo ton table1
;;ean den exo common vars epistrefo ton table1

(defn commons-minus [common-vars sorted-vars1 sorted-vars2 table1 table2]
  (let [sorted-vars1-set (into #{} sorted-vars1)
        common-vars2 (into [] (filter (partial contains? sorted-vars1-set) sorted-vars2))
        common-vars2-set (into #{} common-vars2)
        not-common-vars2 (filter (fn [var-name]
                                   (not (contains? common-vars2-set var-name)))
                                 sorted-vars2)
        not-common-vars2-count (count not-common-vars2)
        common-vars-indexes2 (map (partial library.util/get-var-position sorted-vars2) common-vars)
        table-hash (get-table-hash-line-keys common-vars-indexes2 table2)
        lines-keys (keys table1)]
    (loop [lines-keys lines-keys
           table1 table1]
      (if (empty? lines-keys)
        table1
        (let [line-key (first lines-keys)
              line1 (get table1 line-key)
              table-hash-key (library.util/sort-line-by-sorted-vars common-vars2 sorted-vars1 line1)]
          (if (contains? table-hash table-hash-key)
            (recur (rest lines-keys) (dissoc table1 line-key))
            (recur (rest lines-keys) table1)))))))

(defn empty-vars-minus [sorted-vars1 sorted-vars2 table1 table2]
  (cond
    (and (empty? sorted-vars2) (empty? sorted-vars1))
    {"sorted-vars" []
     "table" {}}
    (empty? sorted-vars1)
    {"sorted-vars" []
     "table" {}}
    (empty? sorted-vars2)
    {"sorted-vars" sorted-vars1
     "table" table1}
    :else {}))


(defn minus [table-info1 table-info2]
  (let [sorted-vars1 (get table-info1 "sorted-vars")
        sorted-vars2 (get table-info2 "sorted-vars")
        table1 (get table-info1 "table")
        table2 (get table-info2 "table")
        common-vars (clojure.set/intersection (into #{} sorted-vars1) (into #{} sorted-vars2))]
    (if (empty? common-vars)
      (let [empty-result (empty-vars-minus sorted-vars1 sorted-vars2 table1 table2)]
        (if-not (empty? empty-result)
          empty-result
          {"sorted-vars" sorted-vars1
           "table" table1}))
      (let [table (commons-minus common-vars sorted-vars1 sorted-vars2 table1 table2)]
        {"sorted-vars" sorted-vars1
         "table" table}))))


;;---------------------------------join(and stin query)----------------------------------------


(defn add-table-to-line [line1 final-table table2 table2-keys]
  (loop [table2-keys table2-keys
         index (count final-table)
         final-table final-table]
    (if (empty? table2-keys)
      final-table
      (let [line2 (get table2 (first table2-keys))
            line (into [] (concat line1 line2))]
        (recur (rest table2-keys) (inc index) (assoc final-table index line))))))

;;gia kathe lines tou enos,olo to table tou alou
(defn cartesian-join [table1 table2]
  (let [table2-keys (keys table2)]
    (loop [line1-keys (keys table1)
           index 0
           final-table {}]
      (if (empty? line1-keys)
        final-table
        (let [line1 (get table1 (first line1-keys))]
          (recur (rest line1-keys) (inc index) (add-table-to-line line1 final-table table2 table2-keys)))))))

(defn commons-join [common-vars sorted-vars1 sorted-vars2 table1 table2]
  (let [sorted-vars1-set (into #{} sorted-vars1)
        common-vars2 (into [] (filter (partial contains? sorted-vars1-set) sorted-vars2))
        common-vars2-set (into #{} common-vars2)
        not-common-vars2 (filter (fn [var-name]
                                   (not (contains? common-vars2-set var-name)))
                                 sorted-vars2)
        common-vars-indexes2 (map (partial library.util/get-var-position sorted-vars2) common-vars)
        table-hash (get-table-hash-line-keys common-vars-indexes2 table2)
        lines-keys (keys table1)]
    (loop [lines-keys lines-keys
           final-table {}]
      (if (empty? lines-keys)
        (let [sorted-vars (into [] (concat sorted-vars1 not-common-vars2))]
          [sorted-vars final-table])
        (let [line1 (get table1 (first lines-keys))
              table-hash-key (library.util/sort-line-by-sorted-vars common-vars2 sorted-vars1 line1)]
          (if (contains? table-hash table-hash-key)
            (let [table2-lines-map (get table-hash table-hash-key)
                  final-table (add-lines-table final-table line1 common-vars-indexes2 (get table2-lines-map "lines"))]
              (recur (rest lines-keys) final-table))
            (recur (rest lines-keys)  final-table)))))))

(defn join [table-info1 table-info2]
  (let [sorted-vars1 (get table-info1 "sorted-vars")
        sorted-vars2 (get table-info2 "sorted-vars")
        table1 (get table-info1 "table")
        table2 (get table-info2 "table")
        common-vars (clojure.set/intersection (into #{} sorted-vars1) (into #{} sorted-vars2))]
    (if (empty? common-vars)
      (let [empty-result (empty-vars-union-join sorted-vars1 sorted-vars2 table1 table2)]
        (if-not (empty? empty-result)
          empty-result
          (let [table (cartesian-join table1 table2)]
            {"sorted-vars" (into [] (concat sorted-vars1 sorted-vars2))
             "table" table})))
      (let [[sorted-vars table] (commons-join common-vars sorted-vars1 sorted-vars2 table1 table2)]
        {"sorted-vars" sorted-vars
         "table" table}))))


;;----------------------------optional-----------------------------------------------
;;optional simeni
;;1)kane join ean ginete alios bale nil(sta orismata tou deuterou pinaka)
;;  ean den ginete join
;;    ean oi common-vars exoun nil times aferese tis vars autes kai xanadokimase gia join
;;       ean ginete join kane join
;;       alios bale nil
;;    alios bale nil

;;----------------------------------nil-joins------------------------------------------------------
(defn get-vars-indexes [sorted-vars vars]
  (map (partial library.util/get-var-position sorted-vars) vars))

(defn get-sorted-values [sorted-vars var-value-map]
  (into [] (map (fn [var-name] (get var-value-map var-name)) sorted-vars)))

(defn add-lines-table-change-order [table line common-vars-indexes lines joined-vars final-sorted-vars]
  (loop [lines lines
         table table]
    (if (empty? lines)
      table
      (let [line (concat-lines line
                               (library.util/get-not-indexes-subline common-vars-indexes
                                                                     (first lines)))
            line (library.util/sort-line-by-sorted-vars final-sorted-vars joined-vars line)]
        (recur (rest lines)
               (assoc table (state.state/get-new-line-ID) line))))))

(defn add-nil-line-change-order [nil-count joined-vars final-sorted-vars table line]
  (assoc table
    (state.state/get-new-line-ID)
    (library.util/sort-line-by-sorted-vars final-sorted-vars
                                           joined-vars
                                           (concat-lines line (take nil-count (repeat nil))))))

(defn join-nil-hash-table [nil-hash-table-info sorted-vars line joined-table]
  (let [nil-hash-table (get nil-hash-table-info "nil-hash-table")
        var-values-line (zipmap sorted-vars line)
        nil-sorted-vars-common (get nil-hash-table-info "nil-sorted-vars-common")
        joined-vars (get nil-hash-table-info "joined-vars")
        nil-sorted-vars-commons-indexes (get nil-hash-table-info "nil-sorted-vars-common-indexes")
        final-sorted-vars (get nil-hash-table-info "final-sorted-vars")
        nil-sorted-vars-not-commons (get nil-hash-table-info "nil-sorted-vars-not-commons")
        table-hash-key (get-sorted-values nil-sorted-vars-common var-values-line)]
    (if (contains? nil-hash-table table-hash-key)         ;;join will happen
      (let [join-lines (get nil-hash-table table-hash-key)
            joined-table (add-lines-table-change-order joined-table
                                                       line
                                                       nil-sorted-vars-commons-indexes
                                                       join-lines
                                                       joined-vars
                                                       final-sorted-vars)]
        [joined-table table-hash-key])
      [joined-table nil])))



(defn join-lines-left [joined-table nil-hash-tables-info]
  (reduce (fn [joined-table nil-hash-table-info]
            (let [lines-left (get nil-hash-table-info "lines-left")
                  joined-vars (get nil-hash-table-info "joined-vars")
                  nil-sorted-vars-not-commons (get nil-hash-table-info "nil-sorted-vars-not-commons")
                  final-sorted-vars (get nil-hash-table-info "final-sorted-vars")
                  f (partial add-nil-line-change-order joined-table
                             (count nil-sorted-vars-not-commons)
                             joined-vars
                             final-sorted-vars)]
              (reduce f joined-table lines-left)))
          joined-table
          nil-hash-tables-info))

(defn find-lines-left [nil-hash-tables-info]
  (let [nil-hash-tables-info (map (fn [nil-hash-table-info]
                                    (let [joined-keys (get nil-hash-table-info "joined-keys")
                                          nil-hash-table (get nil-hash-table-info "nil-hash-table")
                                          nil-hash-table (apply (partial dissoc nil-hash-table) joined-keys)
                                          lines-left (apply concat (vals nil-hash-table))
                                          joined-vars (get nil-hash-table-info "joined-vars")
                                          final-sorted-vars (get nil-hash-table-info "final-sorted-vars")
                                          nil-sorted-vars-not-commons (get nil-hash-table-info "nil-sorted-vars-not-commons")
                                          nil-hash-table (assoc {} "lines-left" lines-left
                                                                   "joined-vars" joined-vars
                                                                   "final-sorted-vars" final-sorted-vars
                                                                   "nil-sorted-vars-not-commons" nil-sorted-vars-not-commons)]
                                      nil-hash-table))
                                  nil-hash-tables-info)]
    nil-hash-tables-info))

(defn join-nil-hash-tables [nil-hash-tables-info table-info joined-table]
  (let [table (get table-info "table")
        sorted-vars (get table-info "sorted-vars")]
    (loop [lines (vals table)
           joined-table joined-table
           nil-hash-tables-info nil-hash-tables-info]
      (if (empty? lines)
        (let [nil-hash-tables-info (find-lines-left nil-hash-tables-info)
              joined-table (join-lines-left joined-table nil-hash-tables-info)]
          joined-table)
        (let [line (first lines)
              [joined-table - nil-hash-tables-info] (reduce (fn [v nil-hash-table-info]
                                                              (let [[joined-table index nil-hash-tables-info] v
                                                                    [joined-table joined-key] (join-nil-hash-table nil-hash-table-info
                                                                                                                   sorted-vars
                                                                                                                   line
                                                                                                                   joined-table)]
                                                                (if-not (nil? joined-key)
                                                                  (let [new-hash-table-info (get nil-hash-tables-info index)
                                                                        new-hash-table-info (update new-hash-table-info
                                                                                                    "joined-keys"
                                                                                                    conj
                                                                                                    joined-key)]
                                                                    [joined-table (inc index) (assoc nil-hash-tables-info index new-hash-table-info)])
                                                                  [joined-table (inc index) nil-hash-tables-info])))
                                                            [joined-table 0 nil-hash-tables-info]
                                                            nil-hash-tables-info)]
          (recur (rest lines) joined-table nil-hash-tables-info))))))


;;ELENXO ENDEXOMENO OLES OI COMMONS NILS,pou tote einai cartesian
(defn get-nil-hash-tables [nil-tables table-info final-sorted-vars]
  (let [sorted-vars (get table-info "sorted-vars")]
    (loop [index 0
           nil-tables nil-tables]
      (if (= index (count nil-tables))
        nil-tables
        (let [nil-table-info (get nil-tables index)
              nil-sorted-vars (get nil-table-info "sorted-vars")
              nil-table (get nil-table-info "table")
              common-vars (clojure.set/intersection (into #{} nil-sorted-vars) (into #{} sorted-vars))
              nil-sorted-vars-commons (into [] (filter (partial contains? (into #{} sorted-vars)) nil-sorted-vars))
              nil-sorted-vars-commons-indexes (into [] (get-vars-indexes nil-sorted-vars common-vars))
              common-indexes (into [] (get-vars-indexes sorted-vars common-vars))
              nil-hash-table (get-table-hash nil-sorted-vars-commons-indexes nil-table)
              nil-sorted-vars-not-commons (into [] (filter (fn [var-name]
                                                             (not (contains? (into #{} nil-sorted-vars-commons) var-name)))
                                                           nil-sorted-vars))
              joined-vars (into [] (concat sorted-vars nil-sorted-vars-not-commons))
              nil-hash-table-info (assoc {} "nil-hash-table" nil-hash-table
                                            "nil-sorted-vars" nil-sorted-vars
                                            "nil-sorted-vars-common" nil-sorted-vars-commons
                                            "nil-sorted-vars-not-commons" nil-sorted-vars-not-commons
                                            "nil-sorted-vars-common-indexes" nil-sorted-vars-commons-indexes
                                            "joined-vars" joined-vars
                                            "final-sorted-vars" final-sorted-vars
                                            "common-indexes" common-indexes
                                            "joined-keys" #{})]
          (recur (inc index) (assoc nil-tables index nil-hash-table-info)))))))

;;----------------------------------not-nil-joins--------------------------------------------------------------
(defn no-commons-optional [sorted-vars1 sorted-vars2 table1 table2]
  [(into [] (concat sorted-vars1 sorted-vars2)) (add-nils table1 sorted-vars1 {} sorted-vars2 {} true)])

(defn add-lines-table [table line sorted-vars2-commons-indexes join-lines]
  (loop [join-lines join-lines
         table table]
    (if (empty? join-lines)
      table
      (let [line-to-add (concat-lines line
                                      (library.util/get-not-indexes-subline sorted-vars2-commons-indexes
                                                                            (first join-lines)))]
        (recur (rest join-lines)
               (assoc table
                 (state.state/get-new-line-ID)
                 line-to-add))))))

(defn add-nil-line [table line nil-count]
  (assoc table
    (state.state/get-new-line-ID)
    (concat-lines line (take nil-count (repeat nil)))))

(defn add-line-to-nil-tables [sorted-vars sorted-vars-commons-indexes line nil-tables]
  (let [sorted-vars-commons-indexes-set (into #{} sorted-vars-commons-indexes)
        [sorted-vars-keep line-keep -] (reduce (fn [[keep-vars keep-line index] value]
                                                 (if (and (contains? sorted-vars-commons-indexes-set index)
                                                          (nil? value))
                                                   [keep-vars keep-line (inc index)]
                                                   [(conj keep-vars (get sorted-vars index))
                                                    (conj keep-line (get line index))
                                                    (inc index)]))
                                               [[] [] 0]
                                               line)]
    (if-not (contains? nil-tables sorted-vars-keep)
      (assoc nil-tables sorted-vars-keep {(state.state/get-new-line-ID) line-keep})
      (update nil-tables sorted-vars-keep assoc (state.state/get-new-line-ID) line-keep))))

(defn commons-optional [common-vars sorted-vars1 sorted-vars2 table1 table2]
  (let [sorted-vars1-commons (into [] (filter (partial contains? (into #{} sorted-vars2)) sorted-vars1))
        sorted-vars2-commons (into [] (filter (partial contains? (into #{} sorted-vars1)) sorted-vars2))
        sorted-vars2-not-commons (filter (fn [var-name]
                                           (not (contains? (into #{} sorted-vars2-commons) var-name)))
                                         sorted-vars2)
        sorted-vars1-commons-indexes (get-vars-indexes sorted-vars1 common-vars)
        sorted-vars2-commons-indexes (get-vars-indexes sorted-vars2 common-vars)
        table-hash (get-table-hash sorted-vars2-commons-indexes table2)
        lines1 (vals table1)
        sorted-vars (into [] (concat sorted-vars1 sorted-vars2-not-commons))]
    (loop [lines1 lines1
           joined-table {}
           nil-tables {}]
      (if (empty? lines1)
        [sorted-vars joined-table nil-tables]
        (let [line1 (first lines1)
              var-values-line1 (zipmap sorted-vars1 line1)
              table-hash-key (get-sorted-values sorted-vars2-commons var-values-line1)]
          (if (contains? table-hash table-hash-key)         ;;join will happen
            (let [join-lines (get table-hash table-hash-key)
                  joined-table (add-lines-table joined-table
                                                line1
                                                sorted-vars2-commons-indexes
                                                join-lines)]
              (recur (rest lines1) joined-table nil-tables))
            (let [line1-commons (get-sorted-values sorted-vars1-commons var-values-line1)]
              (if (= (.indexOf line1-commons nil) -1)
                (recur (rest lines1)
                       (add-nil-line joined-table line1 (count sorted-vars2-not-commons))
                       nil-tables)
                (recur (rest lines1)
                       joined-table
                       (add-line-to-nil-tables sorted-vars1 sorted-vars1-commons-indexes line1 nil-tables))))))))))


(defn optional [table-info1 table-info2]
  (let [sorted-vars1 (get table-info1 "sorted-vars")
        sorted-vars2 (get table-info2 "sorted-vars")
        table1 (get table-info1 "table")
        table2 (get table-info2 "table")
        common-vars (clojure.set/intersection (into #{} sorted-vars1) (into #{} sorted-vars2))]
    (if (empty? common-vars)
      (let [[sorted-vars table] (no-commons-optional sorted-vars1 sorted-vars2 table1 table2)]
        {"sorted-vars" sorted-vars
         "table" table})
      (let [[sorted-vars table nil-tables] (commons-optional common-vars sorted-vars1 sorted-vars2 table1 table2)
            nil-tables (reduce (fn [tables key]
                             (conj tables {"sorted-vars" key "table" (get nil-tables key)}))
                           []
                           (keys nil-tables))
            nil-hash-tables-info (get-nil-hash-tables nil-tables table-info2 sorted-vars)
            table (join-nil-hash-tables nil-hash-tables-info table-info2 table)]
        {"sorted-vars" sorted-vars
         "table" table}))))