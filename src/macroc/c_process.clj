(ns macroc.c-process
  (:require library.util
            macroq.q-process))

(defn constant-construct? [construct]
  (and (coll? construct)
       (= (count construct) 3)
       (and (keyword? (first construct))
            (or (keyword? (second construct)) (string? (second construct)))
            (or (keyword? (nth construct 2)) (string? (nth construct 2))))))

(defn rdf-construct? [construct]
  (and (coll? construct)
       (= (first construct) :rdf)))

(defn relational-construct? [construct]
  (and (coll? construct)
       (= (count construct) 3)
       (not (empty? (filter #(clojure.string/starts-with? % "?") construct)))))

;;2 idi
;;constant-constructs
;;table-constructs
;;bind-consturcts

(defn seperate-constructs [constructs]
  (loop [constructs constructs
         constant-cs []
         relation-cs []
         bind-cs []
         rdf-cs []]
    (if (empty? constructs)
      {
       "constant-cs"  constant-cs
       "relation-cs"  relation-cs
       "bind-cs"      bind-cs
       "rdf-cs"       rdf-cs
       }
      (let [construct (first constructs)]
        (cond
          (rdf-construct? construct)
          (recur (rest constructs) constant-cs relation-cs bind-cs (conj rdf-cs (into [] construct)))
          (constant-construct? construct)
          (recur (rest constructs) (conj constant-cs (into [] construct)) relation-cs bind-cs rdf-cs)
          (relational-construct? construct)
          (recur (rest constructs) constant-cs (conj relation-cs construct) bind-cs rdf-cs)
          :else
          (recur (rest constructs) constant-cs relation-cs (conj bind-cs construct) rdf-cs))))))

(defn get-constructs-types [constructs]
  (let [constructs-types (seperate-constructs constructs)
        constructs-types (-> constructs-types
                             (update-in ["relation-cs"]
                                        (fn [constructs]
                                            (into [] (map (comp #(into [] %)
                                                                macroq.q-process-triples/make-relation-members-str)
                                                          constructs))))
                             (update-in ["bind-cs"]
                                        (fn [constructs]
                                          (into [] (map (fn [construct]
                                                          (let [code-c (first construct)
                                                                c-id (state.state/get-new-code-ID)
                                                                - (state.state/analyse-query c-id code-c)
                                                                locals (into [] (state.state/get-undefined c-id))
                                                                locals (zipmap locals (map symbol locals))
                                                                - (state.state/add-code-locals c-id locals)]
                                                            [(second construct) (str code-c "_" c-id)]))
                                                        constructs)))))]
    constructs-types))