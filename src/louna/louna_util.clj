(ns louna.louna-util
  (:require state.state
            state.db-settings
            library.util
            library.print-data
            macroc.rdf-out
            [clojure.java.io :as io]))

(defn get-first-line
  [table-info]
  (let [table (get table-info "table")
        lines (vals table)]
    (first lines)))

(defn get-table
  "[[] []] to louna-table."
  [sorted-vars table-vec]
  {"sorted-vars" sorted-vars
   "table" (zipmap (range (count table-vec)) table-vec)})

(defn ? [table-info]
  (not (empty? (get table-info "table"))))

(defn print-table [table-info]
  (let [sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        sorted? (get table-info "sorted?" false)]
    (let [lines (vals table)
          - (println sorted-vars)
          - (reduce (fn [table line]
                          (do (apply prn line)
                              (assoc table (state.state/get-new-line-ID) line)))
                        (if sorted? (sorted-map) {})
                        lines)]
      (println)
      table-info)))

(defn print-db [constructs]
  (library.print-data/print-constructs constructs))

(defn merge-relations [& relations]
  (apply (partial merge-with (fn [& values]
                       (into [] (apply concat values))))
         relations))

(defn set-base-path [path]
  (let [path (if (clojure.string/ends-with? path "/") path (str path "/"))]
    (do (if-not (.exists (io/file path))
          (do (.mkdir (java.io.File. path))
              (.mkdir (java.io.File. (str path "/dbs")))
              (.mkdir (java.io.File. (str path "/tables")))))
        (reset! state.db-settings/base-path path))))

(defn get-base-path []
  @state.db-settings/base-path)

(defn get-dbs-path []
  (state.db-settings/get-dbs-path))

(defn get-rdf-path []
  (state.db-settings/get-rdf-path))

(defn get-tables-path []
  (state.db-settings/get-tables-path))

;;-----------------export database to RDF-------------------------------------------------

(defn db-to-nt
  "Converts a database with files(1 relation/file) to RDF in .nt format"
  [db]
  (let [- (if-not (.exists (clojure.java.io/file (str (state.db-settings/get-rdf-path) db)))
            (.mkdir (java.io.File. (str (state.db-settings/get-rdf-path) db)))
            (if (.exists (clojure.java.io/file (str (state.db-settings/get-rdf-path) db "/" db ".nt")))
              (clojure.java.io/delete-file (java.io.File. (str (state.db-settings/get-rdf-path) db "/" db ".nt")))))
        directory (clojure.java.io/file (str (state.db-settings/get-dbs-path) db))
        relations (filter #(.startsWith % ":") (map #(.getName %) (file-seq directory)))
        namespaces (read-string (slurp (str (state.db-settings/get-dbs-path)
                                            db
                                            "/"
                                            (str db ".ns"))))
        - (dorun (map (fn [filename]
                        (macroc.rdf-out/relation-to-nt db namespaces filename))
                      relations))
        - (spit (java.io.File. (str (get-rdf-path) db "/ns")) namespaces)]))

;;----------------ql remove \n from gen query to be ready to run ---------------------

(defn ql-str [sparql-str]
  (clojure.string/replace sparql-str #"\n" " "))


;;----------------set-get join method----------------------------------------------------

;;"disk" or "mem"

(defn set-join-method [method]
  (state.db-settings/set-join-method method))

(defn get-join-method []
  (state.db-settings/get-join-method))


;;------------------sparql functions--------

(defn prefixed? [v]
  (and (keyword? v)
       (not= (.indexOf (name v) ".") -1)))

(defn blank? [v]
  (and (keyword? v)
       (clojure.string/starts-with? (name v) "_")))