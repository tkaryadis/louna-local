(ns benchmark.benchmark-info
  (:use louna.louna
        louna.louna-util))

;;   Benchmark data(auto-generated)

;;   Table will be
;;     c0            c1                      c9
;;     :n.value0  "value1"          ....    "value9"
;;     :n.value10 "value11" ..............  "value19"
;;     ......


;;   A DB was made by this table
;;   It splited in 10 relations(C0=table key,C1...C9=attributes)
;;   :n.c1=[c0 c1] :nc2=[C0 C2] ...   n.c9=[c0,c9]

;;  The various queries joins this relations and apply filter/binds/operators

;;Test results on core i3 2130m 4gb(~1.5GB free) not ssd

;;Systems that will be compared

;;Louna + Jena  reading RDF .nt files
;;Louna + Jena TDB with data stores in files
;;(Jena TDB is triplestore that indexes etc the data and stores them at binary files
;; Louna does no indexing etc,and stores them in text files)

(defn gen-table [collumns rows]
  (let [sorted-vars (into [] (map (fn [n] (str "c" n)) (range collumns)))]
    (loop [index 0
           cur-row 0
           table {}]
      (if (= cur-row rows)
        {"sorted-vars" sorted-vars
         "table" table}
        (let [line (into [] (map (fn [n]
                                   (if (= (mod n collumns) 0)
                                     (keyword (str "n.value" n))
                                     (str "value" n)))
                                 (range index (+ index collumns))))]
          (recur (+ index collumns) (inc cur-row) (assoc table cur-row line)))))))

(defn save-db [genTable db-name]
  (let [- (c {:c-in [genTable] :c-out [db-name]}
             (:n.c1   ?c0 ?c1)
             (:n.c2   ?c0 ?c2)
             (:n.c3   ?c0 ?c3)
             (:n.c4   ?c0 ?c4)
             (:n.c5   ?c0 ?c5)
             (:n.c6   ?c0 ?c6)
             (:n.c7   ?c0 ?c7)
             (:n.c8   ?c0 ?c8)
             (:n.c9   ?c0 ?c9))]))