(ns bgp-joins.relations-run-filter
  (:require state.state
            bgp.filters-run
            library.util))


;;-------------------------------common-used-functions---------------------------------------

(defn get-hash-key [join-vars-indexes line]
  (reduce (fn [hash-key join-vars-index]
            (conj hash-key (get line join-vars-index)))
          []
          join-vars-indexes))

(defn filter-table [join-lines next-hash-table next-join-vars-indexes]
  (loop [index 0
         next-hash-table next-hash-table]
    (if (= index (count join-lines))
      next-hash-table
      (let [line (get join-lines index)]
        (let [next-hash-table (if (empty? next-join-vars-indexes)
                                (assoc next-hash-table (state.state/get-new-line-ID) line)
                                (let [next-hash-key (get-hash-key next-join-vars-indexes line)]
                                  (if (contains? next-hash-table next-hash-key)
                                    (let [prv-values (get next-hash-table next-hash-key)]
                                      (assoc next-hash-table next-hash-key (conj prv-values line)))
                                    (assoc next-hash-table next-hash-key [line]))))]
          (recur (inc index) next-hash-table))))))

;;----------------------------------------filter-rso-------------------------------------------------------------

(defn get-filter-joined-lines [join-type hash-table r s o]
  (cond  (= join-type "rso") (get hash-table [r s o])
         (= join-type "ros") (get hash-table [r o s])
         (= join-type "sro") (get hash-table [s r o])
         (= join-type "sor") (get hash-table [s o r])
         (= join-type "ors") (get hash-table [o r s])
         (= join-type "osr") (get hash-table [o s r])
         :else (do (prn "Unknown join type") (System/exit 0))))

;;thelo i join var tou table na exi idia timi me tin join vars sto [s o]
(defn triple-filter-rso-r [mem-relation? join-type r rel-vars rel-vals rel-filters
                           next-join-vars-indexes hash-table next-hash-table line]
  (let [[s o] (if mem-relation? line (read-string line))
        joined-lines (get-filter-joined-lines join-type hash-table r s o)
        [r-var s-var o-var] rel-vars
        [- s-val o-val] rel-vals
        [s-filters o-filters so-filters] rel-filters]
    (if (and (not (nil? joined-lines))
             (if s-val
               (= s s-val)
               (bgp.filters-run/run-vars-filters-vectors s-filters [s-var] [s]))
             (if o-val
               (= o o-val)
               (bgp.filters-run/run-vars-filters-vectors o-filters [o-var] [o]))
             (bgp.filters-run/run-vars-filters-vectors so-filters [s-var o-var] [s o]))
      (let [next-hash-table (filter-table joined-lines
                                          next-hash-table
                                          next-join-vars-indexes)]
        next-hash-table)
      next-hash-table)))

(defn triple-filter-rso-relation [mem-relation? q-info relation sorted-vars rel-vars rel-vals rel-filters
                                  hash-table join-vars next-join-vars-indexes next-hash-table r]
  (let [db (get q-info "db")
        [r-var s-var o-var] rel-vars

        ;;nil=> first realation(not previous hash table) true=>subject join    false=>object join
        join-type    (cond  (= join-vars [r-var s-var o-var]) "rso"
                            (= join-vars [r-var o-var s-var]) "ros"
                            (= join-vars [s-var r-var o-var]) "sro"
                            (= join-vars [s-var o-var r-var]) "sor"
                            (= join-vars [o-var r-var s-var]) "ors"
                            (= join-vars [o-var s-var r-var]) "osr"
                            :else (do (prn "Unknown filter join type.") (System/exit 0)))

        lines (if mem-relation?
                (library.util/get-relation-lines q-info r)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r))))

        next-hash-table (reduce (partial triple-filter-rso-r
                                         mem-relation?
                                         join-type
                                         r
                                         rel-vars
                                         rel-vals
                                         rel-filters
                                         next-join-vars-indexes
                                         hash-table)
                                next-hash-table
                                lines)]
    next-hash-table))

;;otan to r einai to map-key to bazo meta to reduce 1 fora
(defn triple-filter-rso [q-info relation sorted-vars hash-table join-vars next-join-vars-indexes]
  (let [rel-vars (get relation "triple")
        [r-var s-var o-var] rel-vars
        rel-vals (get relation "rel-vals")
        [r-val s-val o-val] rel-vals
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])
        rel-filters [s-filters o-filters so-filters]

        f-rel (get q-info "f-rel")
        m-rel (get q-info "m-rel")

        f-rel (if (contains? f-rel r-val)
                [r-val]
                (bgp.filters-run/run-many-1var-filter-vectors r-var f-rel (get filters #{r-var} [])))
        m-rel (if (contains? m-rel r-val)
                [r-val]
                (bgp.filters-run/run-many-1var-filter-vectors r-var m-rel (get filters #{r-var} [])))

        next-hash-table {}
        next-hash-table  (reduce (partial triple-filter-rso-relation
                                          false
                                          q-info
                                          relation
                                          sorted-vars
                                          rel-vars
                                          rel-vals
                                          rel-filters
                                          hash-table
                                          join-vars
                                          next-join-vars-indexes)
                                 next-hash-table
                                 f-rel)

        next-hash-table  (reduce (partial triple-filter-rso-relation
                                          true
                                          q-info
                                          relation
                                          sorted-vars
                                          hash-table
                                          join-vars
                                          next-join-vars-indexes)
                                 next-hash-table
                                 m-rel)]
    next-hash-table))