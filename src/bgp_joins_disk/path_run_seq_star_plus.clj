(ns bgp-joins-disk.path-run-seq-star-plus
  (:require library.util
            state.state))

;;------------------or-path----------------------------------------------------------------------
;;or-path to ensomatosa stis proigoumnes
;;i sxesi tha einai ite   o/s/so

;;init (done)
;;s => opos to init tou rs alla xoris to r
;;o => opos to init tou ro alla xoris to r
;;so => opos to init tou rso alla xoris to r
;;value akoma kai ean einai 1 stixio einai vector,giati einai line kai perni kai binds

;;filter(done)
;;opos to filter tou ro/rs/rso xoris to r
;;value ean einai 1 var ro/rs xoris to r => oxi vector

;;join
;;so sto join(mono i so mpori na simetexi se join) => kalo rso
;;mono i so mpori na einai join kai tha einai join me key ite to s ite to o,kai line xoris to o


;;----------------seq-path(rel-rel-rel-...)---------------------------------------------------------------------


(defn triple-s-seq [rdf-dir relation])

(comment
;;oles einai so ektos apo tin teleutea
(defn triple-s-seq [rdf-dir relation]
  (let [query (get relation "query")
        s (second query)
        o (nth query 2)
        parts (get-in relation ["property" "prop-value"] )
        - (prn parts)
        new-vars (map (fn [part] (let [v (louna.state/get-new-var-ID)] [v v])) (rest parts))
        queries-line (into [] (flatten (concat (list s) (interleave parts new-vars) (list (last parts)) (list o))))
        [- queries] (reduce (fn [[query queries] v]
                              (if (= (count query) 2)
                                [[] (conj queries (conj query (symbol v)))]
                                [(conj query (symbol v)) queries]))
                            [[] []]
                            (conj queries-line ""))
        queries (map (fn [query] (list (get query 1) (get query 0) (get query 2))) queries)
        ;- (prn (bgp.bgp-run/run-bgp rdf-dir queries [] []))
        - (prn queries)
        - (System/exit 0)]
    queries))

)
;;;;------------------------------------s+ or o+-----------------------------------------------------


;;knows
;;tha to trexo oso exo agnostous,kathe fora pou kano match to kratao
;;s a:paperA  matched         s ginete set (xero oti ola mou kanoun)

;;unmatched  =>

;;s1 set last-gnostos
;;ean o last-gnostos mou prosferi enan agnosto sinexizo alios sbino tin grami
;;(ean prosferi gnosto i den prosferi kanenan svino tin grami)
;;ean o last-gnostos mou proferi termatiko tote to s1 egiro epistefo  s1


(defn triple-s-path-init [s? star? o-val [matched-set unmathed-table relation-map] line]
  (let [tokens (read-string line)
        s (if s? (first tokens) (second tokens))
        o (if s? (second tokens) (first tokens))
        new-relation-map (if (contains? relation-map s)
                           (let [prv-value (get relation-map s)
                                 new-value (if (vector? prv-value)
                                             (conj prv-value o)
                                             (conj [prv-value] o))]
                             (assoc relation-map s new-value))
                           (assoc relation-map s o))]
    (if (= o o-val)
      [(conj matched-set s) unmathed-table new-relation-map]
      [(if star? (conj matched-set s) matched-set)
       (assoc unmathed-table (state.state/get-new-line-ID) [s #{} o]) new-relation-map])))

(defn add-new [unmatched-table matched-set s previous-set last-value new-value o-val star?]
  (let [matched-set (if (and star? (not (nil? new-value))) (conj matched-set new-value) matched-set)]
    (if (or (nil? new-value) (contains? previous-set new-value))
      [matched-set unmatched-table]
      (if (= new-value o-val)
        [(conj matched-set s) unmatched-table]
        [matched-set
         (assoc unmatched-table
           (state.state/get-new-line-ID)
           [s (conj previous-set last-value) new-value])]))))

(defn check-line [unmatched-table matched-set line relation-map o-val star?]
  (let [s (get line 0)
        previous-set (get line 1)
        last-value (get line 2)
        new-value (get relation-map last-value)]
    (if (coll? new-value)
      (loop [index 0
             matched-set matched-set
             unmatched-table unmatched-table]
        (if (= index (count new-value))
          [matched-set unmatched-table]
          (let [[new-matched-set new-unmatched-table] (add-new unmatched-table
                                                               matched-set
                                                               s
                                                               previous-set
                                                               last-value
                                                               (get new-value index)
                                                               o-val
                                                               star?)]
            (recur (inc index) new-matched-set new-unmatched-table))))
      (add-new unmatched-table matched-set s previous-set last-value new-value o-val star?))))

(defn set-to-table [matched-set s-filters s-var binds-f]
  (reduce (fn [table s]
            (if (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
              (let [line [s]
                    line-binds (binds-f line)]
                (if-not (empty? line-binds)
                  (assoc table (state.state/get-new-line-ID) line-binds)
                  table))
              table))
          {}
          matched-set))

(defn triple-s-or-o-seq-star-plus [q-info relation binds-f s? star?]
  (let [db (get q-info "db")
        o-val (if s? (nth (get relation "query") 2) (second (get relation "query")))
        s-var (if s? (get relation "s-var") (get relation "o-var"))
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        r (first (get-in relation ["property" "prop-value"]))
        rdr (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r))
        [matched-set unmatched-table relation-map] (reduce (partial triple-s-path-init s? star? o-val)
                                                           [#{} {} {}]
                                                           (line-seq rdr))]
    (loop [unmatched-table-keys (keys unmatched-table)
           matched-set matched-set
           unmatched-table unmatched-table]
      (if (empty? unmatched-table)
        (if (not (nil? binds-f))
          (set-to-table matched-set s-filters s-var binds-f)
          matched-set)
        (let [line-key (first unmatched-table-keys)
              line (get unmatched-table line-key)
              [new-matched-table new-unmatched-table] (check-line (dissoc unmatched-table line-key)
                                                                  matched-set
                                                                  line
                                                                  relation-map
                                                                  o-val
                                                                  star?)]
          (recur (keys new-unmatched-table) new-matched-table new-unmatched-table))))))


;;----------------------------------------so+--------------------------------------------------------

;;so+ ti simeni?
;;simeni oti matched einai ola,arkei na ta balo mia fora
;;ara sto matched bazo mono otan den brisko alo gnosto

(defn triple-so-path-init [[unmathed-table relation-map] line]
  (let [tokens (read-string line)
        s (first tokens)
        o (second tokens)
        new-relation-map (if (contains? relation-map s)
                           (let [prv-value (get relation-map s)
                                 new-value (if (vector? prv-value)
                                             (conj prv-value o)
                                             (conj [prv-value] o))]
                             (assoc relation-map s new-value))
                           (assoc relation-map s o))]
    [(assoc unmathed-table (state.state/get-new-line-ID) [s #{o} o]) new-relation-map]))

(defn add-set-values [matched-set s previous-set]
  (reduce (fn [matched-set m] (conj matched-set [s m])) matched-set previous-set))

(defn add-new-so [unmatched-table matched-set s previous-set last-value new-value]
  ;;ean teliosa tote bazo sto match set to [s previous_set]
  (if (or (nil? new-value) (contains? previous-set new-value))
    (if (empty? previous-set)
      [matched-set unmatched-table]
      [(add-set-values matched-set s previous-set) unmatched-table])
    [matched-set
     (assoc unmatched-table
       (state.state/get-new-line-ID)
       [s (conj previous-set last-value new-value) new-value])]))

(defn check-line-so [unmatched-table matched-set line relation-map]
  (let [s (get line 0)
        previous-set (get line 1)
        last-value (get line 2)
        new-value (get relation-map last-value)]
    (if (coll? new-value)
      (loop [index 0
             matched-set matched-set
             unmatched-table unmatched-table]
        (if (= index (count new-value))
          [matched-set unmatched-table]
          (let [[new-matched-set new-unmatched-table] (add-new-so unmatched-table
                                                                  matched-set
                                                                  s
                                                                  previous-set
                                                                  last-value
                                                                  (get new-value index))]
            (recur (inc index) new-matched-set new-unmatched-table))))
      (add-new-so unmatched-table matched-set s previous-set last-value new-value))))

(defn set-to-table-so [matched-set s-var o-var s-filters o-filters so-filters]
  (reduce (fn [table [s o]]
            (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
                     (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
                     (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
              (assoc table (state.state/get-new-line-ID) [s o])
              table))
          {}
          matched-set))

(defn set-star-so [matched-set]
  (first (reduce (fn [[new-matched-set added] v]
                   (let [new-matched-set (conj new-matched-set v)
                         new-matched-set (if (not (contains? added (first v)))
                                           (conj new-matched-set [(first v) (first v)])
                                           new-matched-set)
                         new-matched-set (if (not (contains? added (second v)))
                                           (conj new-matched-set [(second v) (second v)])
                                           new-matched-set)]
                     [new-matched-set (conj added (first v) (second v))]))
                 [#{} #{}]
                 matched-set)))

(defn set-to-map-so [matched-set map-key s-var o-var s-filters o-filters so-filters]
  (reduce (fn [relation-map [s o]]
            (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
                     (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
                     (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
              (cond
                (= map-key "s")
                (if (contains? relation-map s)
                  (let [prv-value (get relation-map s)
                        new-value (if (vector? prv-value)
                                    (conj prv-value o)
                                    (conj [prv-value] o))]
                    (assoc relation-map s new-value))
                  (assoc relation-map s o))
                (= map-key "o")
                (if (contains? relation-map o)
                  (let [prv-value (get relation-map o)
                        new-value (if (vector? prv-value)
                                    (conj prv-value s)
                                    (conj [prv-value] s))]
                    (assoc relation-map o new-value))
                  (assoc relation-map o s)))
              relation-map))
          {}
          matched-set))

(defn set-to-filtered-set [matched-set s-var o-var s-filters o-filters so-filters]
  (reduce (fn [f-set [s o]]
            (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
                     (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
                     (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
              (conj f-set (state.state/get-new-line-ID) [s o])
              f-set))
          #{}
          matched-set))



;;Perfomance :FILTRA STA S KAI O ta exo bali sto telos
;;            EPISIS STO TELOS KANOUN TIN METATROPI
;;read-type init=0 filter=1 join=2
(defn triple-so-seq-star-plus [q-info relation read-type map-key star?]
  (let [db (get q-info "db")
        r (first (get-in relation ["property" "prop-value"]))
        [s-var o-var] (get relation "pair")
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])
        rdr (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r))
        [unmatched-table relation-map] (reduce triple-so-path-init
                                                [{} {}]
                                                (line-seq rdr))]
    (loop [unmatched-table-keys (keys unmatched-table)
           matched-set #{}
           unmatched-table unmatched-table]
      (if (empty? unmatched-table)
        (let [matched-set (if star? (set-star-so matched-set) matched-set)]
          (cond
            (= read-type 0)
            (set-to-table-so matched-set s-var o-var s-filters o-filters so-filters)
            (= read-type 1)
            (set-to-filtered-set matched-set s-var o-var s-filters o-filters so-filters)
            (= read-type 2)
            (set-to-map-so matched-set map-key s-var o-var s-filters o-filters so-filters)))
        (let [line-key (first unmatched-table-keys)
              line (get unmatched-table line-key)
              [new-matched-table new-unmatched-table] (check-line-so (dissoc unmatched-table line-key)
                                                                  matched-set
                                                                  line
                                                                  relation-map)]
          (recur (keys unmatched-table) new-matched-table new-unmatched-table))))))