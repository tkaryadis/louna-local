(ns macroc.c-save
  (:require [clojure.java.io :as io])
  (:import java.io.BufferedWriter
           java.io.FileWriter
           java.io.File))

(defn db-exists? [db]
  (let [db-path (str (state.db-settings/get-dbs-path) db)
        exists? (.exists (io/as-file db-path))]
    exists?))


(defn create-db [db namespaces]
  (do (.mkdir (File. (str (state.db-settings/get-dbs-path) db)))
      (spit (str (state.db-settings/get-dbs-path) db "/" db ".stats") "{}")
      (spit (str (state.db-settings/get-dbs-path) db "/" db ".ns") namespaces)))

(defn update-stats-disk [db constructs namespaces]
  (let [db-stats (read-string (slurp (str (state.db-settings/get-dbs-path) db "/" db ".stats")))
        db-stats (reduce (fn [db-stats relation]
                           (update  db-stats
                                    relation
                                    (fn [prv-lines]
                                      (let [nlines (count (get constructs relation))]
                                        (if (nil? prv-lines)
                                          nlines
                                          (+ prv-lines nlines))))))
                         db-stats
                         (keys constructs))
        - (spit (str (state.db-settings/get-dbs-path) db "/" db ".stats") db-stats)
        - (spit (str (state.db-settings/get-dbs-path) db "/" db ".ns") namespaces)]))

(defn conflict-prefixes? [ns1 ns2]
  (let [prefix1 (into #{} (keys ns1))
        prefix2 (into #{} (keys ns2))
        common-prefixes (clojure.set/intersection prefix1 prefix2)]
    (if (empty? common-prefixes)
      false
      (reduce (fn [bool prefix]
                  (let [conflict? (not (= (get ns1 prefix) (get ns2 prefix)))]
                    (if conflict?
                      (reduced true)
                      false)))
              false
              common-prefixes))))

;;About merging namespaces
;;The data in the databases are saved with the prefix-URI pairs in prv-namespaces
;;the constructs are build with the prefix-URI pairs in cur-namespaces
;;if same prefix and same URI its ok
;;else cannot save
(defn save-c [db constructs]
  (let [prv-ns (if (db-exists? db)
                         (read-string (slurp (str (state.db-settings/get-dbs-path)
                                                  db
                                                  "/"
                                                  db
                                                  ".ns")))
                         {})
        cur-ns (get constructs :ns {})
        constructs (dissoc constructs :ns :size)
        - (if-not (db-exists? db)
            (create-db db cur-ns))]
    (if (or (conflict-prefixes? prv-ns cur-ns) (conflict-prefixes? (clojure.set/map-invert prv-ns)
                                                                   (clojure.set/map-invert cur-ns)))
      (println "Constructs can't be saved.The prefix-URI pair must be the same in DB and in new Costructs\n
                Use the prefix-URI pairs already used in DB or export database and re-import it.")
      (loop [constructs-keys (keys constructs)]
        (if (empty? constructs-keys)
          (update-stats-disk db constructs (merge prv-ns cur-ns))
          (let [relation (first constructs-keys)
                relation-values (get constructs relation)
                writter (BufferedWriter. (FileWriter. (str (state.db-settings/get-dbs-path)
                                                           db
                                                           "/"
                                                           (str relation))
                                                      true))
                - (dorun (map (fn [line]
                                (.write writter (with-out-str (prn line))))
                              relation-values))
                - (.flush writter)
                - (.close writter)]
            (recur (rest constructs-keys))))))))

(defn inc-size [db constructs]
  (let [constructs (update constructs :size inc)
        size (get constructs :size)]
    (if (and (> size state.db-settings/max-construct-size) (not (nil? db)))
      (do (macroc.c-save/save-c db constructs)
          {:size 0})
      constructs)))