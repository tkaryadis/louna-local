(ns bgp.bgp-process
  (:require bgp.bgp-run
            state.db-settings))

;;-----------------------------PROCESS BASIC PATTERN------------------------------------------------
;;--------------------------------------------------------------------------------------------------

(defn split-property [relation property]
  (let [split-pattern (cond
                        (= property "alt")
                        #"\|"
                        (= property "seq")
                        #"-"
                        :else (do (prn "Unknown property type") (System/exit 0)))
        parts-str (clojure.string/split (str relation) split-pattern)
        parts (map library.util/str-keyword-to-keyword parts-str)
        relation (if (= property "alt")
                   (into [] (concat (list "alt") parts))
                   (into [] (concat (list "seq") parts)))]
    (str relation)))

(defn process-property [query]
  (let [r (first query)
        r-str (str r)
        property (cond
                   (not (= (.indexOf r-str "|") -1))
                   "alt"
                   (not (= (.indexOf r-str "-") -1))
                   "seq"
                   (not (= (.indexOf r-str "+") -1))
                   "seq+"
                   (not (= (.indexOf r-str "*") -1))
                   "seq*"
                   :else false)]
    (if (not property)
      query
      (let [s (second query)
            o (nth query 2)]
        (cond
          (or (= property "alt") (= property "seq"))
          (list (split-property r property) s o)

          (= property "seq+")
          (let [new-r (library.util/str-keyword-to-keyword (subs r-str 0 (dec (.length r-str))))]
            (list (str ["seq+" new-r]) s o))

          (= property "seq*")
          (let [new-r (library.util/str-keyword-to-keyword (subs r-str 0 (dec (.length r-str))))]
            (list (str ["seq*" new-r]) s o))
          :else
          (list r s o))))))


(defn process-bgp [q-info queries]
  (loop [queries queries
         relational-queries []
         bind-queries []
         filter-queries []]
    (if (empty? queries)
      (bgp.bgp-run/run-bgp q-info
                           relational-queries
                           bind-queries
                           filter-queries)
      (let [query (first queries)
            query-first (first query)]
        (cond
          (= query-first 1)                                 ;;relation
          (recur (rest queries)
                 (conj relational-queries (process-property (rest query)))
                 bind-queries
                 filter-queries)
          (= query-first 2)                                 ;;bind
          (recur (rest queries)
                 relational-queries
                 (let [[bind-var bind-code] (rest query)
                       query (vector bind-var bind-code)]
                   (conj bind-queries query))
                 filter-queries)
          :else                                             ;;filter
          (recur (rest queries)
                 relational-queries
                 bind-queries
                (conj filter-queries (second query))))))))

