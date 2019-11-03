(ns macroc.c-run
  (:require library.util
            bgp.bgp-process
            bgp.functions-process
            library.print-data
            macroc.c-rdf-in
            macroc.c-save))

(defn add-line-to-constructs [db constructs relation line]
  (if (or (= (get line 0) nil) (= (get line 1) nil))
    constructs
    (let [constructs (macroc.c-save/inc-size db constructs)]
      (if (contains? constructs relation)
        (update-in constructs [relation] conj line)
        (assoc constructs relation [line])))))

(defn save-line-construct [db constructs vars-values construct-map]
  (let [ctype (get construct-map "ctype")]
    (cond
      (= ctype "")
      (let [line (get construct-map "line")]
        (add-line-to-constructs db constructs (get construct-map "relation") [(get line 1) (get line 2)]))
      (= ctype "so")
      (let [line (get construct-map "line")
            line [(get vars-values (get line 0)) (get vars-values (get line 1))]]
        (add-line-to-constructs db constructs (get construct-map "relation") line))
      (= ctype "s")
      (let [line (get construct-map "line")
            line [(get vars-values (get line 0)) (get line 1)]]
        (add-line-to-constructs db constructs (get construct-map "relation") line))
      (= ctype "o")
      (let [line (get construct-map "line")
            line [(get line 0) (get vars-values (get line 1))]]
        (add-line-to-constructs db constructs (get construct-map "relation") line))
      :else (do (prn "Unkown construct type") (System/exit 0)))))

(defn get-construct-map [construct]
  (let [ctype (library.util/get-relation-type construct)
        construct-map {"relation" (first construct)
                       "ctype"    ctype}]
    (cond
      (= ctype "")
      (assoc construct-map "line" (into [] construct))
      (= ctype "so")
      (assoc construct-map "line" (into [] (map library.util/get-var-name
                                                (filter library.util/qvar? construct))))
      (= ctype "s")
      (assoc construct-map "line" [(library.util/get-var-name (second construct))
                                   (nth construct 2)])
      (= ctype "o")
      (assoc construct-map "line" [(second construct)
                                   (library.util/get-var-name (nth construct 2))]))))


(defn add-relation-constructs [db relation-construct-maps constructs table-info]
  (if (or (empty? table-info) (empty? relation-construct-maps))
    constructs
    (let [sorted-vars (get table-info "sorted-vars")
          table (get table-info "table")]
      (loop [lines (vals table)
             ;;akoma kai adia na einai to contruct i sxesi prepei na mpi panta
             constructs (reduce (fn [constructs rel-c]
                                  (let [relation (get rel-c "relation")]
                                    (if-not (contains? constructs relation)
                                      (assoc constructs relation [])
                                      constructs)))
                                constructs
                                relation-construct-maps)]
        (if (empty? lines)
          constructs
          (let [line (first lines)
                vars-values (zipmap sorted-vars line)
                constructs (reduce (fn [constructs construct-map]
                                     (save-line-construct
                                       db
                                       constructs
                                       vars-values
                                       construct-map))
                                   constructs
                                   relation-construct-maps)]
            (recur (rest lines) constructs)))))))

(defn add-constant-constructs [db constructs constant-construct-maps]
  (reduce (fn [constructs construct-map]
            (save-line-construct
              db
              constructs
              nil
              construct-map))
          constructs
          constant-construct-maps))

;;TODO have to add inc size here also
(defn add-bind-constructs [db constructs bind-cs]
  (reduce (fn [constructs bind-c]
            (let [relation (first bind-c)
                  code (second bind-c)
                  code (bgp.functions-process/add-locals-to-code code)]
              (if (contains? constructs relation)
                (let [prv-lines (get constructs relation)]
                  (assoc constructs relation (reduce (fn [lines line]
                                                       (conj lines line))
                                                     prv-lines
                                                     (eval (read-string code)))))
                (assoc constructs relation (eval (read-string code))))))
          constructs
          bind-cs))

(defn add-rdf-constructs [db constructs rdf-cs]
  (if (empty? rdf-cs)
    constructs
    (macroc.c-rdf-in/load-rdf-constructs db constructs (second (first rdf-cs)))))