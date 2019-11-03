(ns bgp-joins-disk.relations-run-shared
  (:require bgp.filters-run)
  (:import (java.io File)))

;;--------------------CONSTANT--------------------------------------------

(defn triple-const [q-info relation]
  (let [db (get q-info "db")
        query (get relation "query")
        s-val (second query)
        o-val (nth query 2)
        r (get relation "relation")

        file-relation? (.exists (File. (str (state.db-settings/get-dbs-path) db "/" r)))
        rdr (if file-relation?
              (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" r)))
        f-lines (if file-relation? (line-seq rdr) [])
        mem-relation? (library.util/mem-relation? q-info r)
        m-lines (if mem-relation? (library.util/get-relation-lines q-info r) [])

        f-member? (reduce (fn [- line]
                          (let [[s o] (read-string line)]
                            (if (and (= o o-val) (= s s-val))
                              (reduced true)
                              false)))
                        false
                        f-lines)

        m-member? (reduce (fn [- line]
                            (let [[s o] line]
                              (if (and (= o o-val) (= s s-val))
                                (reduced true)
                                false)))
                          false
                          m-lines)]
    (or f-member? m-member?)))


;;-------------------------------------------------------------------------

(defn get-r-values [q-info relation property? f-r? m-r?]
  (let [db (get q-info "db")]
    (cond
      property?
      (let [prop (get relation "property")
            prop-type (get prop "prop-type")
            r-values  (cond
                        (or (= prop-type "alt") (= prop-type "seq")) (get prop "prop-value")
                        :else (do (prn "Unknown property type") (System/exit 0)))]
        r-values)
      (and f-r? (not= db ""))
      (let [directory (clojure.java.io/file (str (state.db-settings/get-dbs-path) db))
            r-values (filter #(.contains % ":") (map #(.getName %) (file-seq directory)))
            r-values (map library.util/str-keyword-to-keyword r-values)
            filters  (get relation "filters" {})
            [r-var - -] (get relation "triple")
            r-filters (get filters #{r-var} [])]
        (bgp.filters-run/run-many-1var-filter-vectors r-var r-values r-filters))
      m-r?
      (let [r-values (library.util/get-relations q-info)
            filters  (get relation "filters" {})
            [r-var - -] (get relation "triple")
            r-filters (get filters #{r-var} [])]
        (bgp.filters-run/run-many-1var-filter-vectors r-var r-values r-filters))
      :else [])))


(defn get-relations [db constructs]
  (let [file-r (if (not= db "")
                 (let [directory (clojure.java.io/file (str (state.db-settings/get-dbs-path) db))
                       r-values (filter #(.contains % ":") (map #(.getName %) (file-seq directory)))
                       r-values (map library.util/str-keyword-to-keyword r-values)]
                   r-values)
                 [])
        mem-r  (if (empty? constructs) [] (keys constructs))]
    [file-r mem-r]))