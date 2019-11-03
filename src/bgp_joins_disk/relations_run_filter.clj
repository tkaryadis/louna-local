(ns bgp-joins-disk.relations-run-filter
  (:require library.util
            bgp-joins-disk.relations-run-shared
            bgp.filters-run)
  (:import (java.io File)))

;;san to init mono pou anti table ta bazo se set
;;[r] [o] [s o] [r] [r s] [r o] [r s o]
;;episis den bazo pote binds edo

;;;--------------------s--------------------------------------------------
(defn triple-filter-s-r [construct? s-var o-val s-filters filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= o o-val)
              (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
      (conj filter-set s)
      filter-set)))

(defn triple-filter-s [q-info relation]
  (let [db (get q-info "db")
        s-var (get relation "s-var")
        query (get relation "query")
        o-val (nth query 2)
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var})

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" r)))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r)))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        filter-set (reduce (partial triple-filter-s-r false s-var o-val s-filters)
                           #{}
                           f-lines)
        filter-set (reduce (partial triple-filter-s-r true s-var o-val s-filters)
                           filter-set
                           m-lines)]
    filter-set))


;;---------------o-------------------------------------------------------------

(defn triple-filter-o-r [construct? s-val o-var o-filters filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= s s-val)
              (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
      (conj filter-set o)
      filter-set)))

(defn triple-filter-o [q-info relation]
  (let [db (get q-info "db")
        o-var (get relation "o-var")
        query (get relation "query")
        s-val (second query)
        filters  (get relation "filters" {})
        o-filters (get filters #{o-var})

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" r)))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r)))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        filter-set (reduce (partial triple-filter-o-r false s-val o-var o-filters)
                           #{}
                           f-lines)
        filter-set (reduce (partial triple-filter-o-r true s-val o-var o-filters)
                           filter-set
                           m-lines)]
    filter-set))

;;---------------------------so------------------------------------------------

(defn triple-filter-so-r [construct? s-var o-var s-filters o-filters so-filters filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (let [line [s o]]
        (conj filter-set line))
      filter-set)))


(defn triple-filter-so [q-info relation]
  (let [db (get q-info "db")
        pair (get relation "pair")
        [s-var o-var] pair
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var})
        o-filters (get filters #{o-var})
        so-filters (get filters #{s-var o-var})

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" (get relation "relation"))))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" (get relation "relation"))))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])


        filter-set (reduce (partial triple-filter-so-r false s-var o-var s-filters o-filters so-filters)
                           #{}
                           f-lines)
        filter-set (reduce (partial triple-filter-so-r true s-var o-var s-filters o-filters so-filters)
                           filter-set
                           m-lines)]
    filter-set))

;;-------------------r--------------------------------------------------------------

(defn triple-filter-r [q-id relation]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation false true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation false false true)
        query (get relation "query")
        s-val (second query)
        o-val (nth query 2)

        f-r-values (filter (fn [r-value]
                             (let [query-const {"query" (list r-value s-val o-val)
                                                "relation" r-value}]
                               (bgp-joins-disk.relations-run-shared/triple-const q-id query-const)))
                           f-r-values)

        m-r-values (filter (fn [r-value]
                             (let [query-const {"query" (list r-value s-val o-val)
                                                "relation" r-value}]
                               (bgp-joins-disk.relations-run-shared/triple-const q-id query-const)))
                           m-r-values)]
    (into #{} (concat f-r-values m-r-values))))

;;----------------rs----------------------------------------------------------------

(defn triple-filter-rs-r [construct? r-value s-var o-val s-filters property? filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= o o-val)
              (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
      (let [line (if property? s [r-value s])]
        (conj filter-set line))
      filter-set)))

(defn triple-filter-rs-relation [construct? q-info relation property? filter-set r-value]
  (let [db (get q-info "db")
        [- s-var] (if property?
                    [nil (get relation "s-var")]
                    (get relation "triple"))
        o-val (nth (get relation "query") 2)
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var})
        p-f (partial triple-filter-rs-r
                     construct?
                     r-value
                     s-var
                     o-val
                     s-filters
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f filter-set lines)))


;;thelo sta var-domains kai tin relation
(defn triple-filter-rs [q-info relation property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? false true)
        filter-set (reduce (partial triple-filter-rs-relation false q-info relation property?)
                           #{}
                           f-r-values)
        filter-set (reduce (partial triple-filter-rs-relation false q-info relation property?)
                           filter-set
                           m-r-values)]
    filter-set))

;;-----------------------------ro---------------------------------------------------------

(defn triple-filter-ro-r [construct? r-value s-val o-var o-filters property? filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= s s-val)
              (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
      (let [line (if property? o [r-value o])]
        (conj filter-set line))
      filter-set)))

(defn triple-filter-ro-relation [construct? q-info relation property? filter-set r-value]
  (let [db (get q-info "db")
        [- o-var] (if property?
                    [nil (get relation "o-var")]
                    (get relation "triple"))
        s-val (second (get relation "query"))
        filters  (get relation "filters" {})
        o-filters (get filters #{o-var})
        p-f (partial triple-filter-ro-r
                     construct?
                     r-value
                     s-val
                     o-var
                     o-filters
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f filter-set lines)))


;;thelo sta var-domains kai tin relation
(defn triple-filter-ro [q-info relation property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? false true)
        filter-set (reduce (partial triple-filter-ro-relation false q-info relation property?)
                           #{}
                           f-r-values)
        filter-set (reduce (partial triple-filter-ro-relation true q-info relation property?)
                           filter-set
                           m-r-values)]
    filter-set))


;;----------------------rso------------------------------------------------------------------

(defn triple-filter-rso-r [construct? r-value s-var o-var s-filters o-filters so-filters property? filter-set line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (let [line (if property? [s o] [r-value s o])]
        (conj filter-set line))
      filter-set)))

(defn triple-filter-rso-relation [construct? q-info relation property? filter-set r-value]
  (let [db (get q-info "db")
        [- s-var o-var] (if property?
                          (let [pair (get relation "pair")]
                            [nil (first pair) (second pair)])
                          (get relation "triple"))
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var})
        o-filters (get filters #{o-var})
        so-filters (get filters #{s-var o-var})
        p-f (partial triple-filter-rso-r
                     construct?
                     r-value
                     s-var
                     o-var
                     s-filters
                     o-filters
                     so-filters
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f filter-set lines)))


;;otan to r einai to map-key to bazo meta to reduce 1 fora
(defn triple-filter-rso [q-info relation property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? false true)
        filter-set (reduce (partial triple-filter-rso-relation false q-info relation property?)
                            #{}
                            f-r-values)
        filter-set (reduce (partial triple-filter-rso-relation true q-info relation property?)
                           filter-set
                           m-r-values)]
    filter-set))