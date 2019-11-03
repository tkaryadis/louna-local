(ns macroql.sparql-f
  (:require macroql.to-sparql))

;;-------------------------------------------Util functions-------------------------------------

(defn macro? [s s-macro]
  (not (clojure.core/= s s-macro)))

(defn expand-macro [s]
  (let [s-macro (macroexpand s)]
    (cond
      (macro? s s-macro)
      s-macro
      (symbol? s)
      s
      (string? s)
      (clojure.core/str "\"" s "\"")
      (keyword? s)
      (macroql.to-sparql/sparql-token s)
      :else
      s-macro)))

(defn add-operators
  ([l op]
   (let [l-op (drop-last (interleave l (repeat op)))
         l-str (clojure.core/str "(" (apply clojure.core/str l-op) ")")]
     l-str))
  ([l op f]
   (let [l-op (drop-last (interleave l (repeat op)))
         l-str (clojure.core/str f "(" (apply clojure.core/str l-op) ")")]
     l-str)))

;;-----------------------------Operators.Comparison--------------------------------------------

(defmacro = [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "(" s1 " = " s2 ")")))


(defmacro not= [s1 s2]
  (not (= s1 s2)))

(defmacro < [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "(" s1 " < " s2 ")")))

(defmacro > [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "(" s1 " > " s2 ")")))

(defmacro <= [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "(" s1 " <= " s2 ")")))

(defmacro >= [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "(" s1 " >= " s2 ")")))


;;-----------------------------Operators.Logical--------------------------------------------


(defmacro not [s1]
  (let [s1 (expand-macro s1)]
    (clojure.core/str "(!" s1 ")")))

(defmacro or [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l " || ")]
    l-str))

(defmacro and [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l " && ")]
    l-str))


;;-----------------------------Operators.Arithmetic--------------------------------------------

(defmacro + [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l "+")]
    l-str))

(defmacro - [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l "-")]
    l-str))

(defmacro * [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l "*")]
    l-str))

(defmacro / [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l "/")]
    l-str))


;;-----------------------------------Strings-------------------------------------------------------

(defmacro subs [s1 start end]
  (let [s1 (expand-macro s1)
        start (expand-macro start)
        end (expand-macro end)]
    (clojure.core/str "SUBSTR(" s1 "," "(" start "+ 1)" "," "(" end "-" start "))")))

(defmacro starts-with? [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "STRSTARTS(" s1 "," s2 ")")))

(defmacro ends-with? [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "STRENDS(" s1 "," s2 ")")))

(defmacro  strafter [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "STRAFTER(" s1 "," s2 ")")))


(defmacro strbefore [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "STRBEFORE(" s1 "," s2 ")")))

(defmacro replace [s1 s2 s3]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)
        s3 (expand-macro s3)]
    (clojure.core/str "REPLACE(" s1 "," s2 "," s3 ")")))

(defmacro upper-case [s1]
  (let [s1 (expand-macro s1)]
    (clojure.core/str "UCASE(" s1 ")")))

(defmacro lower-case [s1]
  (let [s1 (expand-macro s1)]
    (clojure.core/str "LCASE(" s1 ")")))

(defmacro strlen [s1]
  (let [s1 (expand-macro s1)]
    (clojure.core/str "STRLEN(" s1 ")")))

(defmacro str [& l]
  (let [l (map expand-macro l)
        l-str (add-operators l "," "CONCAT")]
    l-str))

(defmacro includes? [s1 s2]
  (let [s1 (expand-macro s1)
        s2 (expand-macro s2)]
    (clojure.core/str "CONTAINS(" s1 "," s2 ")")))

;;-----------------------------------aggregates----------------------------------------------------------------

(defmacro sum [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "SUM(" col-macro ")")))

(defmacro avg [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "AVG(" col-macro ")")))

(defmacro min [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "MIN(" col-macro ")")))

(defmacro max [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "MAX(" col-macro ")")))

(defmacro count [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "COUNT(" col-macro ")")))


;;-----------------------------------Sort-by-------------------------------------------------------------------

(defmacro desc [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "DESC(" col-macro ")")))

(defmacro asc [col]
  (let [col-macro (macroexpand col)]
    (clojure.core/str "ASC(" col-macro ")")))


;;------------------------------Numerics------------------------------------------------------------------------

(defmacro abs [n]
  (let [n (macroexpand n)]
    (clojure.core/str "ABS(" n ")")))

(defmacro rand []
  (clojure.core/str "RAND()"))

(defmacro floor [n]
  (let [n (macroexpand n)]
    (clojure.core/str "FLOOR(" n ")")))

(defmacro ceil [n]
  (let [n (macroexpand n)]
    (clojure.core/str "CEIL(" n ")")))

(defmacro round [n]
  (let [n (macroexpand n)]
    (clojure.core/str "ROUND(" n ")")))


;;------------------------------------------------Control-flow----------------------------------------------------

(defmacro iff [condition thenExpression elseExpression]
  (let [condition (expand-macro condition)
        thenExpression (expand-macro thenExpression)
        elseExpression (expand-macro elseExpression)]
    (clojure.core/str "IF(" condition "," thenExpression "," elseExpression ")")))


;;-----------------------------------------------Time/Date-------------------------------------------------------------

(defmacro now []
  (clojure.core/str "NOW()"))

(defmacro hour [n]
  (let [n (macroexpand n)]
    (clojure.core/str "HOUR(" n ")")))

(defmacro timezone [n]
  (let [n (macroexpand n)]
    (clojure.core/str "TIMEZONE(" n ")")))

(defmacro year [n]
  (let [n (macroexpand n)]
    (clojure.core/str "YEAR(" n ")")))

(defmacro month [n]
  (let [n (macroexpand n)]
    (clojure.core/str "MONTH(" n ")")))

(defmacro day [n]
  (let [n (macroexpand n)]
    (clojure.core/str "DAY(" n ")")))

;;----------------------------------------------Types---------------------------------------------------

(defmacro add-type [s type]
  (let [s (expand-macro s)
        type (expand-macro type)]
    (clojure.core/str s "^^" type)))

(defmacro parseInt [s]
  (let [s (expand-macro s)]
    (clojure.core/str "xsd:integer(" s ")")))