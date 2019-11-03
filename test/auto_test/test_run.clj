(ns auto-test.test-run
  (:require state.db-settings))

(defn vec-remove  [coll pos]
  (vec (concat (subvec coll 0 pos) (subvec coll (inc pos)))))

(defn test-tables [table1 table2]
  (let [lines1 (into [] (map (partial into #{}) (vals table1)))
        lines2 (into [] (map (partial into #{}) (vals table2)))]
    (loop [lines1 lines1
           lines2 lines2
           equal? (= (count lines1) (count lines2))]
      (if (or (empty? lines1) (not equal?))
        equal?
        (let [cur-line (first lines1)
              index (.indexOf lines2 cur-line)
              equal? (>= index 0)]
          (if equal?
            (let [lines2 (vec-remove lines2 index)]
              (recur (rest lines1) lines2 equal?))
            (recur (rest lines1) lines2 equal?)))))))


(defn test-queries [q1 q2]
  (let [sorted-vars1 (get q1 "sorted-vars")
        sorted-vars2 (get q2 "sorted-vars")
        table1 (get q1 "table")
        table2 (get q2 "table")]
    (and (= (into #{} sorted-vars1) (into #{} sorted-vars2))
         (test-tables table1 table2))))

(defn test-q-maps [correct-q-map qmap]
  (if (not= (count correct-q-map) (count qmap))
    ["-"]
    (loop [correct-keys (keys correct-q-map)
           failed-queries []]
      (if (empty? correct-keys)
        failed-queries
        (let [correct-query (get correct-q-map (first correct-keys))
              query (get qmap (first correct-keys))
              correct? (test-queries correct-query query)]
          (if correct?
            (recur (rest correct-keys) failed-queries)
            (recur (rest correct-keys) (conj failed-queries (first correct-keys)))))))))

(defn save-q-map [q-map filename]
  (spit (str @state.db-settings/base-path "test-results/" filename) q-map))

(defn test-q-map [qmap filename ]
  (let [correct-q-map (read-string (slurp (str @state.db-settings/base-path "test-results/" filename)))
        failed-queries (test-q-maps correct-q-map qmap)]
    (if (empty? failed-queries)
      (println "-----------------Tests passed!------------------")
      (if (= failed-queries ["-"])
        (println "------------Tests failed.Not same number of queries----------")
        (do (println "----------------Tests failed.Queries failed------------------")
            (dorun (map println failed-queries)))))))