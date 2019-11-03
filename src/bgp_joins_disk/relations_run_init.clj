(ns bgp-joins-disk.relations-run-init
  (:require library.util
            bgp.filters-run
            state.state)
  (:import (java.io File)))



;;;--------------------s--------------------------------------------------
(defn triple-init-s-r [construct? s-var o-val s-filters binds-f table line]
  (let [[s o] (if construct? line (read-string line))]
    (if (and (= o o-val)
             (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
      (let [line [s]
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))

(defn triple-init-s [q-info relation binds-f]
  (let [db (get q-info "db")
        s-var (get relation "s-var")
        query (get relation "query")
        o-val (nth query 2)
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" r)))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r)))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        table (reduce (partial triple-init-s-r false s-var o-val s-filters binds-f)
                      {}
                      f-lines)
        table (reduce (partial triple-init-s-r true s-var o-val s-filters binds-f)
                      table
                      m-lines)]
    table))


;;------------------o------------------------------------------------------

(defn triple-init-o-r [construct? s-val o-var o-filters binds-f table line]
  (let [[s o] (if construct? line (read-string line))]
    (if (and (= s s-val)
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
      (let [line [o]
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))

(defn triple-init-o [q-info relation binds-f]
  (let [db (get q-info "db")
        o-var (get relation "o-var")
        query (get relation "query")
        s-val (second query)
        filters (get relation "filters" {})
        o-filters (get filters #{o-var} [])

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" r)))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r)))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        table (reduce (partial triple-init-o-r false s-val o-var o-filters binds-f)
                      {}
                      f-lines)
        table (reduce (partial triple-init-o-r true s-val o-var o-filters binds-f)
                      table
                      m-lines)]
    table))

;;-----------------so---------------------------------------------------------


(defn triple-init-so-r [construct? s-var o-var s-filters o-filters so-filters binds-f table line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (let [line [s o]
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))


(defn triple-init-so [q-info relation binds-f]
  (let [db (get q-info "db")
        pair (get relation "pair")
        [s-var o-var] pair
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])

        r (get relation "relation")
        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" (get relation "relation"))))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" (get relation "relation"))))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        table (reduce (partial triple-init-so-r false s-var o-var s-filters o-filters so-filters binds-f)
                      {}
                      f-lines)
        table (reduce (partial triple-init-so-r true s-var o-var s-filters o-filters so-filters binds-f)
                      table
                      m-lines)]
    table))


;;-------------------r--------------------------------------------------------------

(defn triple-init-r [q-id relation]
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
                           m-r-values)

        table (reduce (fn [table r-value]
                        (assoc table (state.state/get-new-line-ID) [r-value]))
                      {}
                      (concat f-r-values m-r-values))]
    table))


;;----------------rs----------------------------------------------------------------

(defn triple-init-rs-r [construct? r-value s-var o-val s-filters binds-f property? table line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (= o o-val)
             (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
      (let [line (if property? [s] [r-value s])
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))

(defn triple-init-rs-relation [construct? q-info relation binds-f property? table r-value]
  (let [db (get q-info "db")
        [- s-var] (if property?
                    [nil (get relation "s-var")]
                    (get relation "triple"))
        o-val (nth (get relation "query") 2)
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        p-f (partial triple-init-rs-r
                     construct?
                     r-value
                     s-var
                     o-val
                     s-filters
                     binds-f
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f table lines)))


;;thelo sta var-domains kai tin relation
(defn triple-init-rs [q-id relation binds-f property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? false true)
        table (reduce (partial triple-init-rs-relation false q-id relation binds-f property?)
                      {}
                      f-r-values)
        table (reduce (partial triple-init-rs-relation true q-id relation binds-f property?)
                      table
                      m-r-values)]
    table))

;;-----------------------------ro---------------------------------------------------------

(defn triple-init-ro-r [construct? r-value s-val o-var o-filters binds-f property? table line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (= s s-val)
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
      (let [line (if property? [o] [r-value o])
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))

(defn triple-init-ro-relation [construct? q-info relation binds-f property? table r-value]
  (let [db (get q-info "db")
        [- o-var] (if property?
                    [nil (get relation "o-var")]
                    (get relation "triple"))
        s-val (second (get relation "query"))
        filters (get relation "filters" {})
        o-filters (get filters #{o-var} [])
        p-f (partial triple-init-ro-r
                     construct?
                     r-value
                     s-val
                     o-var
                     o-filters
                     binds-f
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f table lines)))


;;thelo sta var-domains kai tin relation
(defn triple-init-ro [q-id relation binds-f property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? false true)
        table (reduce (partial triple-init-ro-relation false q-id relation binds-f property?)
                      {}
                      f-r-values)
        table (reduce (partial triple-init-ro-relation false q-id relation binds-f property?)
                      table
                      m-r-values)]
    table))


;;----------------------rso------------------------------------------------------------------

(defn triple-init-rso-r [construct? r-value s-var o-var s-filters o-filters so-filters binds-f property? table line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (let [line (if property? [s o] [r-value s o])
            line-binds (binds-f line)]
        (if-not (empty? line-binds)
          (assoc table (state.state/get-new-line-ID) line-binds)
          table))
      table)))

(defn triple-init-rso-relation [construct? q-info relation binds-f property? table r-value]
  (let [db (get q-info "db")
        [- s-var o-var] (if property?
                          (let [pair (get relation "pair")]
                            [nil (first pair) (second pair)])
                          (get relation "triple"))
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])
        p-f (partial triple-init-rso-r
                     construct?
                     r-value
                     s-var
                     o-var
                     s-filters
                     o-filters
                     so-filters
                     binds-f
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f table lines)))



;;otan to r einai to map-key to bazo meta to reduce 1 fora
(defn triple-init-rso [q-id relation binds-f property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-id relation property? false true)
        table (reduce (partial triple-init-rso-relation false q-id relation binds-f property?)
                      {}
                      f-r-values)
        table (reduce (partial triple-init-rso-relation true q-id relation binds-f property?)
                      table
                      m-r-values)]
    table))