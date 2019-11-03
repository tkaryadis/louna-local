(ns bgp-joins-disk.relations-run-join
  (:require library.util
            bgp-joins-disk.relations-run-shared
            bgp.filters-run
            [clojure.core.reducers :as r])
  (:import (java.io File)))

;;----------------------so--------------------------------------------------

(defn triple-join-so-r [construct? s-var o-var s-filters o-filters so-filters map-key relation-map line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
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
      relation-map)))

(defn triple-join-so [q-info relation map-key]
  (let [db (get q-info "db")
        pair (get relation "pair")
        [s-var o-var] pair
        filters  (get relation "filters" {})
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

        relation-map {}
        relation-map (reduce (partial triple-join-so-r false s-var o-var s-filters o-filters so-filters map-key)
                             relation-map
                             f-lines)

        relation-map (reduce (partial triple-join-so-r true s-var o-var s-filters o-filters so-filters map-key)
                                relation-map
                                m-lines)]
    relation-map))


;;------------------------rs----------------------------------------------------------------------

(defn triple-join-rs-r [construct? r-value s-var o-val s-filters map-key join-map line]
  (let [tokens (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= o o-val)
              (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
      (cond
        (= map-key "r")
        (let [line [s]]
          (if (contains? join-map r-value)
            (let [prv-value (get join-map r-value)
                  new-value (conj prv-value line)]
              (assoc join-map r-value new-value))
            (assoc join-map r-value [line])))
        (= map-key "s")
        (let [line [r-value]]
          (if (contains? join-map s)
            (let [prv-value (get join-map s)
                  new-value (conj prv-value line)]
              (assoc join-map s new-value))
            (assoc join-map s [line]))))
      join-map)))

(defn triple-join-rs-relation [construct? q-info relation map-key join-map r-value]
  (let [db (get q-info "db")
        triple (get relation "triple")
        [- s-var] triple
        o-val (nth (get relation "query") 2)
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        p-f (partial triple-join-rs-r
                     r-value
                     s-var
                     o-val
                     s-filters
                     map-key)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f join-map lines)))


;;thelo sta var-domains kai tin relation
(defn triple-join-rs [q-info relation map-key]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation false true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation false false true)
        join-map (reduce (partial triple-join-rs-relation false q-info relation map-key)
                         {}
                         f-r-values)
        join-map (reduce (partial triple-join-rs-relation false q-info relation map-key)
                         join-map
                         m-r-values)]
    join-map))


;;-------------------------ro---------------------------------------------------------


(defn triple-join-ro-r [construct? r-value s-val o-var o-filters map-key relation-map line]
  (let [tokens  (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if  (and (= s s-val)
              (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
      (cond
        (= map-key "r")
        (let [line [o]]
          (if (contains? relation-map r-value)
            (let [prv-value (get relation-map r-value)
                  new-value (conj prv-value line)]
              (assoc relation-map r-value new-value))
            (assoc relation-map r-value [line])))
        (= map-key "o")
        (let [line [r-value]]
          (if (contains? relation-map o)
            (let [prv-value (get relation-map o)
                  new-value (conj prv-value line)]
              (assoc relation-map o new-value))
            (assoc relation-map o [line]))))
      relation-map)))

(defn triple-join-ro-relation [construct? q-info relation map-key join-map r-value]
  (let [db (get q-info "db")
        triple (get relation "triple")
        [- o-var] triple
        s-val (second (get relation "query"))
        filters  (get relation "filters" {})
        o-filters (get filters #{o-var} [])
        p-f (partial triple-join-ro-r
                     construct?
                     r-value
                     s-val
                     o-var
                     o-filters
                     map-key)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f join-map lines)))


;;thelo sta var-domains kai tin relation
(defn triple-join-ro [q-info relation map-key]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation false true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation false false true)
        join-map (reduce (partial triple-join-ro-relation false q-info relation map-key)
                         {}
                         f-r-values)
        join-map (reduce (partial triple-join-ro-relation true q-info relation map-key)
                         join-map
                         m-r-values)]
    join-map))


;;---------------------------------rso-----------------------------------------------

(defn triple-join-rso-r [construct? r-value s-var o-var s-filters o-filters so-filters map-key property? join-map line]
  (let [tokens  (if construct? line (read-string line))
        s (first tokens)
        o (second tokens)]
    (if (and (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s])
             (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o])
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (cond                                                 ;;theli ftiaximo bazo vector akoma kai otan 1 line
        (= map-key "s")
        (if property?
          (if (contains? join-map s)
            (let [prv-value (get join-map s)
                  new-value (if (vector? prv-value)
                              (conj prv-value o)
                              (conj [prv-value] o))]
              (assoc join-map s new-value))
            (assoc join-map s o))
          (let [line [r-value o]]
            (if (contains? join-map s)
              (let [prv-value (get join-map s)
                    new-value (conj prv-value line)]
                (assoc join-map s new-value))
              (assoc join-map s [line]))))
        (= map-key "o")
        (if property?
          (if (contains? join-map o)
            (let [prv-value (get join-map o)
                  new-value (if (vector? prv-value)
                              (conj prv-value s)
                              (conj [prv-value] s))]
              (assoc join-map o new-value))
            (assoc join-map o s))
          (let [line [r-value s]]
            (if (contains? join-map o)
              (let [prv-value (get join-map o)
                    new-value (conj prv-value line)]
                (assoc join-map o new-value))
              (assoc join-map o [line]))))
        (= map-key "so")
        (if (contains? join-map [s o])
          (let [prv-value (get join-map [s o])
                new-value (if (vector? prv-value)
                            (conj prv-value r-value)
                            (conj [prv-value] r-value))]
            (assoc join-map [s o] new-value))
          (assoc join-map [s o] r-value))
        (= map-key "r")
        (let [line [s o]]
          (if (contains? join-map r-value)
            (let [prv-value (get join-map r-value)
                  new-value (conj prv-value line)]
              (assoc join-map r-value new-value))
            (assoc join-map r-value [line])))
        (= map-key "rs")
        (let [line [o]]
          (if (contains? join-map [r-value s])
            (let [prv-value (get join-map [r-value s])
                  new-value (conj prv-value line)]
              (assoc join-map [r-value s] new-value))
            (assoc join-map [r-value s] [line])))
        (= map-key "ro")
        (let [line [s]]
          (if (contains? join-map [r-value o])
            (let [prv-value (get join-map [r-value o])
                  new-value (conj prv-value line)]
              (assoc join-map [r-value o] new-value))
            (assoc join-map [r-value o] [line])))
        :else (do (prn "Unknown map key" map-key) (System/exit 0)))
      join-map)))

(defn triple-join-rso-relation [construct? q-info relation map-key property? join-map r-value]
  (let [db (get q-info "db")
        [- s-var o-var] (if property?
                          (let [pair (get relation "pair")]
                            [nil (first pair) (second pair)])
                          (get relation "triple"))
        filters  (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])
        p-f (partial triple-join-rso-r
                     construct?
                     r-value
                     s-var
                     o-var
                     s-filters
                     o-filters
                     so-filters
                     map-key
                     property?)
        lines (if construct?
                (library.util/get-relation-lines q-info r-value)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r-value))))]
    (reduce p-f join-map lines)))


;;otan to r einai to map-key to bazo meta to reduce 1 fora
(defn triple-join-rso [q-info relation map-key property?]
  (let [f-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? true false)
        m-r-values (bgp-joins-disk.relations-run-shared/get-r-values q-info relation property? false true)
        relations-map (reduce (partial triple-join-rso-relation false q-info relation map-key property?)
                              {}
                              f-r-values)
        relations-map (reduce (partial triple-join-rso-relation false q-info relation map-key property?)
                              relations-map
                              m-r-values)]
    relations-map))