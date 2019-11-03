(ns macroq.q-rewrite
  (:require state.state
            library.util))

;;--------------add-operators------------------------------------------------------
;;sta bgp ta and ta bazi i auto-group,edo bazo and metaxi groups
;;opou bro (() ...) => bazo and
(defn add-and-operator [group]
  (let [query (first group)]
    (if (library.util/and-group? query)
      (let [query (concat (list ':and) query)]
        (concat (list query) (rest group)))
      group)))


(defn bind-or-filter? [query]
  (or (= (first query) ':bind) (= (first query) ':filter)))

;;ean bro bind/filter i filter/bind ta enono se 1 me onoma fb
(defn add-fb-operator [group]
  (loop [group group
         bind-filter-queries []]
    (let [query (first group)]
      (if-not (bind-or-filter? query)
        (if-not (empty? bind-filter-queries)
          (let [new-query (into [] (concat (list ':fb) (apply concat (map rest bind-filter-queries))))
                new-group (into [] (concat (list new-query) group))]
            [new-query new-group])
          [query group])
        (recur (rest group) (conj bind-filter-queries query))))))

;;--------------property paths-----------------------------------------------------

(defn inverse-query? [query]
  (and (coll? query)
       (let [r (str (first query))]
         (clojure.string/starts-with? r "%"))))

(defn seq-path-query? [query]
  (if (coll? query)
    (let [r (first query)]
      (and (library.util/ns-token? r)
           (not= (.indexOf (str r) "-") -1)))))


(defn to-initial-form [token]
  (cond
    (and (string? token) (library.util/qvar? token))
    (symbol token)
    (and (string? token) (clojure.string/starts-with? token ":"))
    (library.util/str-keyword-to-keyword token)

    :else token))

;;s part1  part2  part3 o
;;s part1 o1 part2 o2 part3 o
;;s  part1 o1
;;o1 part2
(defn seq-path-expand [query]
  (let [r (str (first query))
        s (second query)
        o (nth query 2)
        parts (clojure.string/split r #"-")
        new-vars (map (fn [part] (let [v (state.state/get-new-var-ID)] [v v])) (rest parts))
        queries-line (into [] (flatten (concat (list s) (interleave parts new-vars) (list (last parts)) (list o))))
        [- queries] (reduce (fn [[query queries] v]
                              (if (= (count query) 2)
                                   [[] (conj queries (conj query (to-initial-form v)))]
                                   [(conj query (to-initial-form v)) queries]))
                              [[] []]
                              (conj queries-line ""))
        queries (map (fn [query] (list (get query 1) (get query 0) (get query 2))) queries)]
    queries))

(defn get-inversed-query [query]
  (let [r (keyword (subs (str (first query)) 2))
        s (second query)
        o (nth query 2)]
    (list r o s)))

(defn property-path-query? [query]
  (or (seq-path-query? query) (inverse-query? query)))

(defn or-property-path? [query]
  (and (coll? query)
       (let [r (str (first query))]
         (not (= (.indexOf r "|") -1)))))

(defn star-property-path? [query]
  (and (coll? query)
       (let [r (str (first query))]
         (or (not= (.indexOf r "+") -1) (not= (.indexOf r "*") -1)))))

(defn property-paths [group]
  (let [query (first group)]
    (cond
      (seq-path-query? query)
      (let [queries (seq-path-expand query)
            first-query (first queries)
            first-query (if (inverse-query? first-query) (get-inversed-query first-query) first-query)
            queries (concat (list first-query) (rest queries))]
        (concat queries (rest group)))

      (inverse-query? query)
      (let [query (get-inversed-query query)]
        (concat (list query) (rest group)))

      :else group)))

;;---------------------------multiple binds---------------------------------------------------------
(defn multiple-binds [query]
  (let [fuc (first query)
        qvars (rest query)
        temp-var (symbol (state.state/get-new-var-ID))
        ]
    (loop [qvars qvars
           index 0
           bind-queries [(list fuc temp-var)]]
      (if (empty? qvars)
        bind-queries
        (let [cur-q (list (list 'get temp-var index) (first qvars))]
          (recur (rest qvars) (inc index) (conj bind-queries cur-q)))))))


(defn multiple-var-binds [group]
  (let [query (first group)]
    (if (library.util/multiple-bind-query-s? query)
      (concat (multiple-binds query) (rest group))
      group)))