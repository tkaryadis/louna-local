(ns library.util
  (:require state.state
            [clojure.java.io :as io]))

;;----------------------seperate-queries-------------------------

;;queries bindOrFilter einai 2(bind) i 3(filter)
(defn get-filter-bind-queries [queries bindOrFilter]
  (reduce (fn [queries-result query] (if (= (first query) bindOrFilter)
                                       (conj queries-result (rest query))
                                       queries-result))
          []
          queries))

;;-------------------read-files----------------------------------
;;proipotheti oti to proto token den exi keno,eno to deutero mpori na exi keno
(defn get-tokes-from-line [line]
  (let [index1 (.indexOf line " ")
        s (subs line 0 index1)
        line (subs line (+ index1 1))
        o (subs line 0)]
    [s o]))




;;-------------------Namespaces----------------------------------
;;---------------------------------------------------------------
(defn colon-namespaced? [token]
  (.contains token ":"))

(defn IRI? [token]
  (clojure.string/starts-with? token "<"))

;;to token einai xoris ta < > kai den telioni se /
(defn get-namespace-value [token]
  (if (not= (.indexOf token "#") -1)
    (let [index (.lastIndexOf token "#")
          namespace (.substring token 0 (+ index 1))
          value (.substring token (+ index 1) (.length token))]
      [namespace value])
    (let [index (.lastIndexOf token "/")
          namespace (.substring token 0 (+ index 1))
          value (.substring token (+ index 1) (.length token))]
      [namespace value])))

(defn ns-query? [query]
  (and (vector? query)
       (= (count query) 2)
       (string? (first query))
       (clojure.string/ends-with? (.trim (first query)) ":")
       ;(not (clojure.string/starts-with? (str (first query)) "?"))
       ;(not (= "*" (str (first query))))
       ;(not (= "distinct" (str (first query))))
       ;(not (coll? (first query)))
       )
  )


;;---------------seperate groups/triples---------------------------
;;-----------------------------------------------------------------


(def operators #{:add :if :not :and :fb :bind :filter :sort-by :group-by :project :do :do-each :table})

(def internal-operators #{"__add__" "__if__" "__not__" "__and__" "__fb__" "__sort-by__" "__group-by__"
                          "__project__" "__do__" "__do-each__" "__table__"})

(defn binary-op? [operator]
  (or (= operator "__add__")
      (= operator "__if__")
      (= operator "__not__")
      (= operator "__and__")))

(defn triple? [query]
  (not (contains? internal-operators (str (first query)))))

(defn basic-pattern? [group]
  (triple? (first group)))

(declare multiple-bind-query-s?)
(declare bind-query-s?)

(defn and-group? [query]
  (and (coll? query)
       (coll? (first query))
       (not (or (multiple-bind-query-s? query)
                (bind-query-s? query)))))

(defn group? [query]
  (let [op (first query)]
    (or (and-group? query) (contains? library.util/operators op))))



(defn change-operator-name [operator]
  (cond
    (= operator ":not") "__not__"
    (= operator ":if")  "__if__"
    (= operator ":add")  "__add__"
    (= operator ":filter") "__filter__"
    (= operator ":bind")   "__bind__"
    (= operator ":fb")     "__fb__"
    (= operator ":and")     "__and__"
    (= operator ":project") "__project__"
    (= operator ":sort-by") "__sort-by__"
    (= operator ":group-by") "__group-by__"
    (= operator ":do") "__do__"
    (= operator ":do-each") "__do-each__"
    (= operator ":table") "__table__"))

;;--------------seperate queries to relational queries/bind/filters---------------
;;--------------------------------------------------------------------------------

(defn qvar? [name]
  (clojure.string/starts-with? (str name) "?"))

(defn literal? [m]
  (and (not (coll? m))
       (or (keyword? m)
           (string? m)
           (number? m)
           (qvar? (str m))
           (nil? (resolve m)))))

;;bind = 2 meli to deutero query var,to proto collection
;;relation query = ola ta meli literals

;;ean do problima sto na xexorizo,bind-query => panta 2 melos nea metabliti
(defn bind-query-s? [query]
  (and (coll? query)
       (= (count query) 2)
       (coll? (first query))
       (clojure.string/starts-with? (str (second query)) "?")))

(defn multiple-bind-query-s? [query]
  (and (coll? query)
       (coll? (first query))
       (> (count query) 2)
       (let [qvars (rest query)]
         (empty? (filter #(not (clojure.string/starts-with? (str %) "?")) qvars)))))

;;(if 2 3)  BUG
(defn relational-query-s? [query]
  (and (coll? query)
       (cond
         (= (count query) 3)
         (and (literal? (first query)) (literal? (second query)) (literal? (nth query 2)))
         (= (count query) 2)
         (and (literal? (first query)) (literal? (second query)))
         :else false)))

(defn ns-token? [token]
  (and (keyword? token)
       (not= (.indexOf (str token) ".") -1)))

(defn black-node? [token]
  (and (keyword? token)
       (clojure.string/starts-with? (name token) "_")))

(defn str-keyword-to-keyword [str-keyword]
  (keyword (.substring str-keyword 1)))



;;-----------------------qvars process----------------------------------
;;----------------------------------------------------------------------



(defn get-var-name [name]
  (if (clojure.string/starts-with? name "?")
    (subs name 1)
    name))

(defn get-vars-query-str [query-str]
  (into #{} (map (fn [cur-var] (subs cur-var 1)) (re-seq #"\?\w+" query-str))))

(defn get-vars-query-str-vector [query-str]
  (into [] (map (fn [cur-var] (subs cur-var 1)) (re-seq #"\?\w+" query-str))))

(defn get-vars [query]
  (map get-var-name (filter qvar? query)))


(defn get-relation-type [query]
  (let [r (first query)
        s (second query)
        o (nth query 2)
        r? (library.util/qvar? r)
        s? (library.util/qvar? s)
        o? (library.util/qvar? o)]
    (cond
      (and r? s? o?) "rso"
      (and r? s?)    "rs"
      (and r? o?)    "ro"
      r?             "r"
      (and s? o? )   "so"
      s?             "s"
      o?             "o"
      :else          "")))                                  ;;ola stathera


#_(defn relation-2var? [query]
  (= (count (get-vars (rest query))) 2))

(defn get-var-position [sorted-vars var-name]
  (loop [index 0]
    (let [cur-var (get sorted-vars index)]
      (if (= cur-var var-name)
        index
        (recur (inc index))))))

(defn sort-vars [sorted-vars vars]
  (into [] (sort-by (partial get-var-position sorted-vars)
                    vars)))

(defn assoc-conj-set [m k v]
  (if (contains? m k)
    (assoc m k (conj (get m k) v))
    (assoc m k #{v})))

;;i line einai diatetagmeni simfona me tis sorted-vars
;;thelo tin line diatetagmeni simfona me ta vars-name
;;in [a b d c] [c a] [0 2]
;;out {c : 2 a:0} (edo exo ali diataxi)
(defn get-vars-values-map [sorted-vars vars-names line]
  (reduce (fn [var-name-values var-name]
            (let [index (get-var-position sorted-vars var-name)
                  var-value (get line index)]
              (assoc var-name-values var-name var-value)))
          {}
          vars-names))

#_(defn relation-query? [query]
  (let [token1 (first query)]
    (and (string? token1) (not (clojure.string/starts-with? token1 "?")))))

(defn value-to-vector [value]
  (if (vector? value) value [value]))

;;apo tin line perno mono ta indexes
(defn get-indexes-subline [indexes line]
  (loop [indexes indexes
         subline []]
    (if (empty? indexes)
      subline
      (recur (rest indexes) (conj subline (get line (first indexes)))))))

;;apo tin line den perno ta indexes
(defn get-not-indexes-subline [indexes line]
  (let [indexes-set (into #{} indexes)]
    (loop [index 0
           subline []]
      (if (= index (count line))
        subline
        (if (contains? indexes-set index)
          (recur (inc index) subline)
          (recur (inc index) (conj subline (get line index))))))))

;;in : [[x y z] [0 1 2] [z x y]]
;;out : [2 0 1]
(defn sort-line-by-sorted-vars [sorted-vars sorted-vars-line line]
  (let [line-map (zipmap sorted-vars-line line)]
    (into [] (map (fn [sorted-var] (get line-map sorted-var)) sorted-vars))))

(defn ttmp-var? [v]
  (clojure.string/starts-with? v "TTMP"))

(defn sort-line-rmv-tmp [sorted-vars sorted-vars-line line]
  (let [line-map (zipmap sorted-vars-line line)]
    (reduce (fn [[sorted-vars sorted-line] sorted-var]
              (if (ttmp-var? sorted-var)
                [sorted-vars sorted-line]
                [(conj sorted-vars sorted-var)
                 (conj sorted-line (get line-map sorted-var))]))
            [[] []]
            sorted-vars)))


(defn get-index-sorted-vars [sorted-vars sorted-vars-index index]
  (let [var-name (get sorted-vars-index index)
        new-index (.indexOf sorted-vars var-name)]
    new-index))

;;sorted-vars1 [x y z]
;;sorted-vars2 [z x y]
;;indexes [0 1] => [z x] =simfona_me_to_1= [2 0]
(defn sort-indexes-by-sorted-vars [sorted-vars1 sorted-vars2 indexes]
  (let [var-indexes (zipmap sorted-vars2 indexes)]
    (into [] (map (fn [index]
                    (library.util/get-var-position sorted-vars1 (get sorted-vars2 index)))
                  indexes))))


;;;------------general library.util--------------------------------------

(defn remove-index [v index]
  (if (= index (- (count v) 1))
    (into [] (concat (subvec v 0 index)))
    (into [] (concat (subvec v 0 index)
                     (subvec v (+ index 1))))))

(defn delete-dir-recursively [dirname]
  (if (.exists (io/file dirname))
    (let [func (fn [func f]
                 (when (.isDirectory f)
                   (doseq [f2 (.listFiles f)]
                     (func func f2)))
                 (clojure.java.io/delete-file f))]
      (func func (clojure.java.io/file dirname)))))


(defn parse-int [number-string]
  (try (Integer/parseInt number-string)
       (catch Exception e nil)))

(defn parse-double [number-string]
  (try (Double/parseDouble number-string)
       (catch Exception e nil)))

(defn string?-number [s]
  (let [i (parse-int s)]
    (if i
      i
      (let [d (parse-double s)]
        (if d
          d
          nil)))))

(defn string?-boolean [s]
  (if (or (= s "true") (= s "false"))
    (read-string s)
    nil))

(defn vector-to-table [v s-var o-var]
  {"sorted-vars" [s-var o-var]
   "table" (reduce (fn [table line] (assoc table (state.state/get-new-line-ID) line))
                   {}
                   v)})

(defn get-typed-token [token]
  (let [number (string?-number token)
        bool (string?-boolean token)]
    (cond
      (clojure.string/starts-with? token ":")
      (keyword (subs 1 token))
      (not (nil? number))
      number
      (not (nil? bool))
      bool
      :else
      token)))


;;--------------macroq-info----------------------------------------------------------------
(defn mem-relation? [q-info relation]
  (let [relation-vector (get-in q-info ["constructs" relation])]
    (not (nil? relation-vector))))

(defn get-relation-lines [q-info relation]
  (let [relation-vector (get-in q-info ["constructs" relation])]
    (if (nil? relation-vector)
      []
      relation-vector)))

(defn get-relations [q-info]
  (into [] (keys (get q-info "constructs"))))

(defn get-relation-nlines [q-info relation]
  (let [cost (get-in q-info ["db-stats" relation])
        cost (if (nil? cost)
               (count (get-in q-info ["constructs" relation]))
               cost)]
    cost))


