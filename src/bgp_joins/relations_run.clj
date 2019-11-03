(ns bgp-joins.relations-run
  (:require library.util
            bgp.filters-run
            state.state
            bgp.binds-run
            bgp-joins.relations-run-filter
            [clojure.core.reducers :as r]))

;;-------------------------------------------Joins!---------------------------------------------------------

(defn get-hash-key [join-vars-indexes line]
  (reduce (fn [hash-key join-vars-index]
            (conj hash-key (get line join-vars-index)))
          []
          join-vars-indexes))

(defn get-joined-lines [join-type hash-table r s o]
  (cond
    (= join-type "")
    [[]]

    (= join-type "s")
    (get hash-table [s])

    (= join-type "o")
    (get hash-table [o])

    (= join-type "so")
    (get hash-table [s o])

    (= join-type "os")
    (get hash-table [o s])

    (= join-type "r")
    (get hash-table [r])

    (= join-type "rs")
    (get hash-table [r s])

    (= join-type "sr")
    (get hash-table [s r])

    (= join-type "ro")
    (get hash-table [r o])

    (= join-type "or")
    (get hash-table [o r])

    :else (do (prn "Unknown join type") (System/exit 0))))


(defn get-next-hash-table1 [sorted-vars join-lines add-var add-value next-hash-table next-join-vars-indexes binds table-filter-functions]
  (loop [index 0
         next-hash-table next-hash-table]
    (if (= index (count join-lines))
      next-hash-table
      (let [join-line (get join-lines index)
            line-to-add (conj join-line add-value)
            passed? (bgp.filters-run/run-table-filter-vectors add-var table-filter-functions sorted-vars line-to-add)
            line-to-add (if passed? (bgp.binds-run/add-binds-to-line sorted-vars binds table-filter-functions line-to-add) [])]
        (if-not (empty? line-to-add)
          (let [next-hash-table (if (empty? next-join-vars-indexes)
                                  (assoc next-hash-table (state.state/get-new-line-ID) line-to-add)
                                  (let [next-hash-key (get-hash-key next-join-vars-indexes line-to-add)]
                                    (if (contains? next-hash-table next-hash-key)
                                      (let [prv-values (get next-hash-table next-hash-key)]
                                        (assoc next-hash-table next-hash-key (conj prv-values line-to-add)))
                                      (assoc next-hash-table next-hash-key [line-to-add]))))]
            (recur (inc index) next-hash-table))
          (recur (inc index) next-hash-table))))))


(defn get-next-hash-table2 [sorted-vars join-lines add-var1 add-value1 add-var2 add-value2 next-hash-table next-join-vars-indexes binds table-filter-functions]
  (loop [index 0
         next-hash-table next-hash-table]
    (if (= index (count join-lines))
      next-hash-table
      (let [join-line (get join-lines index)
            line-to-add (conj join-line add-value1 add-value2)
            passed? (bgp.filters-run/run-table-filter-vectors add-var1 table-filter-functions sorted-vars line-to-add)
            passed? (if passed? (bgp.filters-run/run-table-filter-vectors add-var2 table-filter-functions sorted-vars line-to-add)
                                false)
            line-to-add (if passed? (bgp.binds-run/add-binds-to-line sorted-vars binds table-filter-functions line-to-add)
                                    [])]
        (if-not (empty? line-to-add)
          (let [next-hash-table (if (empty? next-join-vars-indexes)
                                  (assoc next-hash-table (state.state/get-new-line-ID) line-to-add)
                                  (let [next-hash-key (get-hash-key next-join-vars-indexes line-to-add)]
                                    (if (contains? next-hash-table next-hash-key)
                                      (let [prv-values (get next-hash-table next-hash-key)]
                                        (assoc next-hash-table next-hash-key (conj prv-values line-to-add)))
                                      (assoc next-hash-table next-hash-key [line-to-add]))))]
            (recur (inc index) next-hash-table))
          (recur (inc index) next-hash-table))))))


(defn get-next-hash-table-init [line-vars line next-hash-table next-join-vars-indexes binds table-filter-functions]
  (let [line-to-add (bgp.binds-run/add-binds-to-line line-vars binds table-filter-functions line)]
    (if-not (empty? line-to-add)
      (let [next-hash-table (if (empty? next-join-vars-indexes)
                              (assoc next-hash-table (state.state/get-new-line-ID) line-to-add)
                              (let [next-hash-key (get-hash-key next-join-vars-indexes line-to-add)]
                                (if (contains? next-hash-table next-hash-key)
                                  (let [prv-values (get next-hash-table next-hash-key)]
                                    (assoc next-hash-table next-hash-key (conj prv-values line-to-add)))
                                  (assoc next-hash-table next-hash-key [line-to-add]))))]
        next-hash-table)
      next-hash-table)))

;;thelo i join var tou table na exi idia timi me tin join vars sto [s o]
(defn triple-join-rso-r [mem-relation? join-type sorted-vars  add-vars r rel-vars rel-vals rel-filters
                         join-vars next-join-vars-indexes binds table-filter-functions hash-table
                         next-hash-table line]
  (let [[s o] (if mem-relation? line (read-string line))
        joined-lines (get-joined-lines join-type hash-table r s o)
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
      (if (= (count join-vars) 0)                             ;;first relation
        (let [[add-vars add-line] (library.util/sort-line-rmv-tmp add-vars [r-var s-var o-var] [r s o])
              next-hash-table (get-next-hash-table-init add-vars
                                                        add-line
                                                        next-hash-table
                                                        next-join-vars-indexes
                                                        binds
                                                        table-filter-functions)]
          next-hash-table)
        (let [[add-vars add-line] (library.util/sort-line-rmv-tmp add-vars [r-var s-var o-var] [r s o])]
          (cond
            (= (count add-vars) 0)
            (let [next-hash-table (bgp-joins.relations-run-filter/filter-table joined-lines
                                                next-hash-table
                                                next-join-vars-indexes)]
              next-hash-table)

            (= (count add-vars) 1)
            (let [add-var (first add-vars)
                  ;;if empty next-join-vars-indexes(last join) => returns a table(not hashed)
                  next-hash-table (get-next-hash-table1 (conj sorted-vars add-var)
                                                        joined-lines
                                                        add-var
                                                        (first add-line)
                                                        next-hash-table
                                                        next-join-vars-indexes
                                                        binds
                                                        table-filter-functions)]
              next-hash-table)

            (= (count add-vars) 2)
            (let [[add-var1 add-var2] add-vars
                  [add-value1 add-value2] add-line
                   ;;if empty next-join-vars-indexes(last join) => returns a table(not hashed)
                  next-hash-table (get-next-hash-table2 (conj sorted-vars (first add-vars) (second add-vars))
                                                        joined-lines
                                                        add-var1
                                                        add-value1
                                                        add-var2
                                                        add-value2
                                                        next-hash-table
                                                        next-join-vars-indexes
                                                        binds
                                                        table-filter-functions)]
              next-hash-table)
            :else
            (do (prn "Unknown join type") (System/exit 0)))))
      next-hash-table)))



(defn triple-join-rso-relation [mem-relation? q-info relation sorted-vars rel-vars rel-vals rel-filters
                                add-vars hash-table join-vars next-join-vars-indexes table-filter-functions
                                next-hash-table r]
  (let [db (get q-info "db")
        binds (get relation "binds")
        [r-var s-var o-var] rel-vars

        ;;nil=> first realation(not previous hash table) true=>subject join    false=>object join
        join-type    (cond  (empty? join-vars)          ""
                            (= join-vars [s-var])       "s"
                            (= join-vars [o-var])       "o"
                            (= join-vars [s-var o-var]) "so"
                            (= join-vars [o-var s-var]) "os"
                            (= join-vars [r-var])       "r"
                            (= join-vars [r-var s-var]) "rs"
                            (= join-vars [r-var o-var]) "ro"
                            (= join-vars [s-var r-var]) "sr"
                            (= join-vars [o-var r-var]) "or"
                            :else (do (prn "Unknown join type") (System/exit 0)))

        lines (if mem-relation?
                (library.util/get-relation-lines q-info r)
                (line-seq (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r))))


        next-hash-table (reduce (partial triple-join-rso-r
                                         mem-relation?
                                         join-type
                                         sorted-vars
                                         add-vars
                                         r
                                         rel-vars
                                         rel-vals
                                         rel-filters
                                         join-vars
                                         next-join-vars-indexes
                                         binds
                                         table-filter-functions
                                         hash-table)
                                next-hash-table
                                lines)]
    next-hash-table))


;;otan to r einai to map-key to bazo meta to reduce 1 fora
(defn triple-join-rso [q-info relation sorted-vars  add-vars hash-table join-vars next-join-vars-indexes table-filter-functions]
  (let [rel-vars (get relation "triple")
        [r-var s-var o-var] rel-vars
        rel-vals (get relation "rel-vals")
        [r-val s-val o-val] rel-vals
        filters (get relation "filters" {})
        s-filters (get filters #{s-var} [])
        o-filters (get filters #{o-var} [])
        so-filters (get filters #{s-var o-var} [])
        rel-filters [s-filters o-filters so-filters]

        ;;meni na bgalo entelos to filro apo relation.process
        f-rel (get q-info "f-rel")
        m-rel (get q-info "m-rel")

        f-rel (if (contains? f-rel r-val)
                [r-val]
                (bgp.filters-run/run-many-1var-filter-vectors r-var f-rel (get filters #{r-var} [])))
        m-rel (if (contains? m-rel r-val)
                [r-val]
                (bgp.filters-run/run-many-1var-filter-vectors r-var m-rel (get filters #{r-var} [])))

        next-hash-table  {}
        next-hash-table  (reduce (partial triple-join-rso-relation
                                          false
                                          q-info
                                          relation
                                          sorted-vars
                                          rel-vars
                                          rel-vals
                                          rel-filters
                                          add-vars
                                          hash-table
                                          join-vars
                                          next-join-vars-indexes
                                          table-filter-functions)
                                 next-hash-table
                                 f-rel)

        next-hash-table  (reduce (partial triple-join-rso-relation
                                          true
                                          q-info
                                          relation
                                          sorted-vars
                                          rel-vars
                                          rel-vals
                                          rel-filters
                                          add-vars
                                          hash-table
                                          join-vars
                                          next-join-vars-indexes
                                          table-filter-functions)
                                 next-hash-table
                                 m-rel)]
    next-hash-table))