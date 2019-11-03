(ns macroql.to-sparql
  (:require macroq.q-process))

(defn spaces [brackets]
  (apply str (take (* brackets 2) (repeat " "))))

(defn sparql-token [token]
  (cond
    (clojure.string/starts-with? (str token)  ":")
    (let [keyword-str (subs (str token) 1)
          parts (clojure.string/split keyword-str #"\.")
          ]
      (str (get parts 0) ":" (get parts 1)))
    (string? token)
    (str "\"" token "\"")
    :else token))

(defn get-sparql-triple [triple brackets]
  (let [r (first triple)
        s (second triple)
        o (nth triple 2)]
    (str (spaces brackets) (sparql-token s) " " (sparql-token r) " " (sparql-token o) " .\n")))

(defn add-group? [group]
  (loop [group group
         add? false]
    (if (empty? group)
      add?
      (let [cur-group (first group)]
        (if-not (library.util/group? cur-group)
          (recur (rest group) false)
          (let [operator (str (first cur-group))]
            (cond
              (= operator ":add")
              (recur (rest group) true)
              :else false)))))))

(defn where-arg [group brackets]
  (let [add? (add-group? group)]
    (loop [group group
           sparql-str (if add? (str (spaces brackets) "{\n") "")]
      (if (empty? group)
        (str sparql-str (spaces (dec brackets)) "}\n")
        (let [cur-group (first group)]
          (if-not (library.util/group? cur-group)
            (cond
              (library.util/relational-query-s? cur-group)
              (recur (rest group) (str sparql-str (get-sparql-triple cur-group brackets)))
              (library.util/bind-query-s? cur-group)
              (let [bind-function (first cur-group)
                    bind-var (second cur-group)]
                (recur (rest group) (str sparql-str (spaces brackets) "BIND (" (macroexpand bind-function) " AS " bind-var ") .\n")))
              :else
              (let []
                (recur (rest group) (str sparql-str (spaces brackets) "FILTER (" (macroexpand cur-group) ") .\n"))))
            (let [operator (str (first cur-group))
                  new-group (rest cur-group)
                  new-group-str (where-arg new-group (if (= operator ":add") brackets (+ brackets 1)))]
              (cond
                (= operator ":if")
                (recur (rest group) (str sparql-str "\n" (spaces brackets) "OPTIONAL\n" (spaces brackets)  "{\n" new-group-str))
                (= operator ":not")
                (recur (rest group) (str sparql-str "\n" (spaces brackets) "MINUS\n"  (spaces brackets) "{\n" new-group-str))
                (= operator ":add")
                (recur (rest group) (str sparql-str "}\n" (spaces brackets) "UNION\n"  (spaces brackets) "{\n" new-group-str))))))))))

(defn where []
  "\nWHERE\n{\n")

(defn select [distinct?]
  (if distinct?
    "SELECT DISTINCT "
    "SELECT "))

(defn select-arg [project]
  (reduce (fn [s a] (str s a " ")) "" project))

(defn vec-remove [coll pos]
  (if (>= pos 0)
    (vec (concat (subvec coll 0 pos) (subvec coll (inc pos))))
    coll))

(defn get-search-op-list [group search-op]
  (loop [index 0
         search-index -1]
    (if (or (>= search-index 0) (= index (count group)))
      [(vec-remove group search-index)
       (if (= search-index -1) [] (get group search-index))]
      (let [cur-group (get group index)]
        (if-not (library.util/group? cur-group)
          (recur (inc index) search-index)
          (let [operator (str (first cur-group))]
            (if (= operator search-op)
              (recur (inc index) index)
              (recur (inc index) search-index))))))))

(defn spaces-between-str [col]
  (apply str (interleave col (take (count col) (repeat " ")))))

(defn group-by-args [group-by-list]
  (if (or (empty? group-by-list)
          (= (first (rest group-by-list)) '-))
    ""
    (let [args (rest group-by-list)
          novars? (library.util/bind-query-s? (first args))
          [group-by-vars binds filters] (macroq.q-process-op-args/seperate-group-by-arguments args)
          group-by-vars (into [] (map (comp symbol (partial str "?")) group-by-vars))
          having-args (map macroexpand filters)
          ]
      [(if novars?
         ""
         (str "\nGROUP BY " (spaces-between-str group-by-vars)
              "\nHAVING " (spaces-between-str having-args)))
       (map (fn [bind] (str "(" (macroexpand (first bind)) " AS " (second bind) ")")) binds)])))

(defn sort-args [sort-by-list]
  (let [sort-by-args (rest sort-by-list)
        sort-by-args (map macroexpand sort-by-args)]
    (str "\nORDER BY " (spaces-between-str sort-by-args))))

(defmacro ql [& queries]
  (let [[settings queries] (macroq.q-process/get-settings queries)
        [project queries] (macroq.q-process/get-project queries)
        [project distinct?] (if (contains? (into #{} project) 'distinct)
                              [(into [] (rest project)) true]
                              [project false])

        project (into [] (map (fn [b] (if (coll? b) (str "(" (macroexpand (first b)) " AS " (second b) ")") b)) project))
        group (into [] queries)

        [group group-by-list] (get-search-op-list group ":group-by")
        [group-by-str binds]  (group-by-args group-by-list)

        project (if (and (= project ['*]) (not (empty? binds))) [] project)


        [group sort-by-list] (get-search-op-list group ":sort-by")
        sort-by-str (if-not (empty? sort-by-list) (sort-args sort-by-list) "")

        limit? (= (str (first (peek group))) ":limit")
        [group limit-str] (if limit? [(pop group) (str "\nLIMIT " (second (peek group)))] [group ""])


        select-str (str (select distinct?) (select-arg project) (spaces-between-str binds))
        where-str (str (where) (where-arg group 1))

        sparql-str (str select-str where-str group-by-str sort-by-str limit-str)]
    sparql-str))