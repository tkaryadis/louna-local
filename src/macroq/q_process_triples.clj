(ns macroq.q-process-triples
  (:require library.util))

;;-----------------------------------------------------------------------------
;;  ?person ->  "\"?person\""
(defn replace-var [str-query query-var]
  (clojure.string/replace str-query
                          (re-pattern (str "[^\"]\\?" query-var "\\W"))
                          (fn [str-var]
                            (let [first-char (.substring  str-var 0 1)
                                  last-char (.substring  str-var (dec (.length str-var)))]
                              (str first-char "\"" (subs str-var 1 (dec (.length str-var))) "\" " last-char)))))

;;thelo na bro tin query-varOXIWORD kai na tin antikatastasiso me query-varOXIWORD
(defn qvars-to-str [query]
  (let [str-query (str query)
        query-vars (library.util/get-vars-query-str-vector str-query)
        str-query (reduce replace-var str-query query-vars)]
    str-query))

(defn make-relation-members-str [query]
  (map (fn [member]
         (cond
           (or (library.util/qvar? (str member))
               (and (clojure.string/starts-with? (str member) "<")
                    (clojure.string/ends-with? (str member) ">"))
               (= member '-))
           (str member)
           :else
           member))                                         ;;eval member?
       query))

(defn bind-members-str [query]
  (let [bind-var (library.util/get-var-name (str (second query)))
        bind-code (qvars-to-str (first query))
        c-id (state.state/get-new-code-ID)
        - (state.state/analyse-query c-id query)
        locals (into [] (state.state/get-undefined c-id))
        locals (zipmap locals (map symbol locals))
        - (state.state/add-code-locals c-id locals)]
    (if (empty? locals)
      (do (state.state/remove-code c-id)
          (into [] (list bind-var (str bind-code))))
      (into [] (list bind-var (str bind-code "_" c-id))))))

;;1 relation-query
;;2 bind-query
;;3 filter-query
(defn make-query-str [query]
  (cond
    (library.util/relational-query-s? query)
    (into [] (concat (list 1) (make-relation-members-str query)))

    (library.util/bind-query-s? query)
    (into [] (concat (list 2) (bind-members-str query)))

    :else
    (let [c-id (state.state/get-new-code-ID)
          - (state.state/analyse-query c-id query)
          locals (into [] (state.state/get-undefined c-id))
          locals (zipmap locals (map symbol locals))
          - (state.state/add-code-locals c-id locals)]
      (if (empty? locals)
        (do (state.state/remove-code c-id)
            (into [] (list 3 (str (qvars-to-str query)))))
        (into [] (list 3 (str (qvars-to-str query) "_" c-id)))))))