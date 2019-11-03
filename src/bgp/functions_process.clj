(ns bgp.functions-process)


;;---------------------query to vectors------------------------------------

;;ta locals tha ta paro apo ta undefined pou ta exo idi
;;undefined = {1 {"money" 10}}
;;get-in code-id name
(defn replace-locals [code-str code-id locals]
  (loop [locals-keys (keys locals)
         code-str code-str]
    (if (empty? locals-keys)
      (subs code-str 0 (- (.length code-str) (+ (.length (str code-id)) 1)))
      (let [local (first locals-keys)
            code-str (clojure.string/replace code-str
                                    (re-pattern (str local "\\W"))
                                    (fn [str-var]
                                      (let [last-char (.substring  str-var (dec (.length str-var)))]
                                        (str " (get-in @state.state/undefined " [code-id local] " )" last-char))))]
        (recur (rest locals-keys) code-str)))))

(defn get-code-id [code-str]
  (let [last-index (.lastIndexOf code-str "_")]
    (if (= last-index -1)
      -1
      (let [code-id-str (subs code-str (inc last-index) (.length code-str))
            code-id (library.util/string?-number code-id-str)]
        (if (nil? code-id)
          -1
          code-id)))))

(defn add-locals-to-code [code-str]
  (let [code-id (get-code-id code-str)]
    (if (= code-id -1)
      code-str
      (let [locals (state.state/get-undefined code-id)]
        (if (empty? locals)
          code-str
          (replace-locals code-str code-id locals))))))

(defn get-vars-filter-function [filter-query]
  (let [                                                    ;- (prn "macroq-vector" filter-query)
        filter-query (add-locals-to-code filter-query)
        query-str (if-not (string? filter-query) (str filter-query) filter-query)
        query-vars (library.util/get-vars-query-str-vector query-str)
        dep-vars (into #{} query-vars)
        query-parts (clojure.string/split query-str #"\"\?\w+\"")
        vars-filter-function (conj (into [] (interleave query-parts query-vars)) (last query-parts))
        ;vars-filter-function (conj vars-filter-function code-id)
        ]
    ;;query-vector px  ["(and (clojure.string/starts-with? " "lastname" " \"pa\") (not (= " "continent" " \"europe\")) " "lastname" " " "city" ")"]
    ;;stis 1,3,5,7...einai oi metablites (antikathisto,apply str,readstring eval)
    [dep-vars vars-filter-function]))

(defn get-filter-query-vars-sorted [sorted-vars query-vars]
  (let [vars (into #{} query-vars)
        query-vars-sorted (into [] (filter (partial contains? vars) sorted-vars))]
    query-vars-sorted))

(defn table-filter-query-to-function [sorted-vars filter-query]
  (let [filter-query (add-locals-to-code filter-query)
        query-str (if-not (string? filter-query) (str filter-query) filter-query)
        query-vars (library.util/get-vars-query-str-vector query-str)
        dep-vars-sorted (into [] (get-filter-query-vars-sorted sorted-vars query-vars))
        dep-vars (into #{} dep-vars-sorted)
        query-parts (clojure.string/split query-str #"\"\?\w+\"")
        tree-filter-function (conj (into [] (interleave query-parts query-vars)) (last query-parts))
        filter-var (peek dep-vars-sorted)]
    ;;query-vector px  ["(and (clojure.string/starts-with? " "lastname" " \"pa\") (not (= " "continent" " \"europe\")) " "lastname" " " "city" ")"]
    ;;stis 1,3,5,7...einai oi metablites (antikathisto,apply str,readstring eval)
    [filter-var dep-vars tree-filter-function]))


(defn get-bind-function [bind-code]
  (let [bind-code-str (str bind-code)
        bind-code-str (add-locals-to-code bind-code-str)
        query-vars (library.util/get-vars-query-str-vector bind-code-str)
        query-parts (clojure.string/split bind-code-str #"\"\?\w+\"")
        bind-function (conj (into [] (interleave query-parts query-vars)) (last query-parts))]
    ;;vector einai stis thesis 1,3,5 ... einai oi vars san strings
    bind-function))


;;-----------------vector to f-eval----------------------------------------------------

(defn get-vector-vars [f-vector]
  (let [var-positions (into #{} (range 1 (count f-vector) 2))
        vars (reduce (fn [vars pos] (conj vars (get f-vector pos))) #{} var-positions)]
    vars))

(defn vector-to-function [f-vector]
  (let [f-args (into [] (get-vector-vars f-vector))
        f-body (apply str f-vector)
        f-eval (eval (read-string (str "(fn " (into [] (map symbol f-args)) f-body ")")))]
    [f-args f-eval]))

(defn vector-to-f-eval [f-vector]
  (let [[f-args f-eval] (vector-to-function f-vector)]
    {"f-args" f-args
     "f-eval" f-eval
     "query" (apply str (map-indexed (fn [i m] (if (odd? i) (str "?" m) m)) f-vector))}))


(defn run-f-eval [vars-values f-map]
  (let [f-eval (get f-map "f-eval")
        f-args (get f-map "f-args")
        args-values (map (fn [m] (get vars-values m)) f-args)
        result (apply f-eval args-values)]
    result))


;;------------------------vars-filters-to-vars-filters-eval--------------------------------------

(defn get-filters-f-eval-map [filters]
  (reduce (fn [f k]
            (let [filters-vec (get filters k)]
              (assoc f k (into [] (map bgp.functions-process/vector-to-f-eval filters-vec)))))
          {}
          (keys filters)))

(defn get-table-filters-f-eval-map [table-filter-functions]
  (reduce (fn [f k]
            (let [filters-map (get table-filter-functions k)
                  filters-vec (get filters-map "functions")
                  filter-vec (into [] (map bgp.functions-process/vector-to-f-eval filters-vec))
                  filters-map (assoc filters-map "functions" filter-vec)]
              (assoc f k filters-map)))
          {}
          (keys table-filter-functions)))