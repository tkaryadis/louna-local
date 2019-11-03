(ns macroc.rdf-out
  (:require library.util))

;;------------------------table prefix names,to table URI names ------------------------------------------

(defn change-namespaces [d-q-namespaces line]
  (reduce
    (fn [ns-line token]
      (if (keyword? token)
        (cond
          (library.util/black-node? token)
          (conj ns-line token)
          :else
          (let [[rns-key const-name] (clojure.string/split (name token) #"\.")
                ns-pair (get d-q-namespaces rns-key)
                new-token (if (= (first ns-pair) "")
                            (str (second ns-pair) const-name)
                            (keyword (str (first ns-pair) "." const-name)))]
            (conj ns-line new-token)))
        (conj ns-line token)))
    []
    line))

(defn get-URI-table [table-info]
  (let [                                                    ;d-namespaces (read-string (slurp (str (state.db-settings/get-dbs-path) db "/" db ".ns")))
        sorted-vars (get table-info "sorted-vars")
        table (get table-info "table")
        sorted? (get table-info "sorted?" false)
        change-namespaces-partial (partial change-namespaces {})] ;;!!!!!!!!!!!!!
    (let [lines (vals table)
          table (reduce (fn [table line]
                          (let [q-ns-line (change-namespaces-partial line)]
                            (assoc table (state.state/get-new-line-ID) q-ns-line)))
                        (if sorted? (sorted-map) {})
                        lines)]
      {"sorted-vars" sorted-vars
       "sorted?" sorted?
       "table" table})))

;;---------------------------DB to .nt file-----------------------------------------------------------

(defn token-to-URI [namespaces token position]
  (cond
    (library.util/ns-token? token)
    (let [parts (clojure.string/split (subs (str token) 1) #"\.")
          prefix (get parts 0)
          suffix (get parts 1)
          prefix-uri (get namespaces prefix)
          prefix-uri (if (nil? prefix-uri)
                       "<http://louna/ns/"
                       (str "<" prefix-uri))]
      (str prefix-uri suffix "> "))
    (string? token)
    (if (= position 2)
      (str "\"" token "\"")
      (str "<http://louna/string/" token "> "))
    (and (number? token) (integer? token))
    (if (= position 2)
      (str "\"" token "\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      (str "<http://louna/number/" token "> "))
    (and (number? token) (not (integer? token)))
    (if (= position 2)
      (str "\"" token "\"^^<http://www.w3.org/2001/XMLSchema#float>")
      (str "<http://louna/number/" token "> "))
    (boolean? token)
    (if (= position 2)
      (str "\"" token "\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
      (str "<http://louna/boolean/" token "> "))
    (keyword? token)
    (str "<http://louna/keyword/"
         (subs (str token) 1) "> ")
    :else
    (str "<http://louna/unknown/" token "> ")))

(defn get-nt-line [namespaces relation line]
  (str (token-to-URI namespaces (get line 0) 0)
       (token-to-URI namespaces (library.util/str-keyword-to-keyword relation) 1)
       (token-to-URI namespaces (get line 1) 2)))

(defn relation-to-nt [db namespaces filename]
  (with-open [rdr (clojure.java.io/reader (str (state.db-settings/get-dbs-path) db "/" filename))
              wrtr (clojure.java.io/writer (str (state.db-settings/get-rdf-path)
                                                db
                                                "/"
                                                db
                                                ".nt")
                                           :append true)]
    (loop [lines (line-seq rdr)]
      (let [line (first lines)]
        (if (or (empty? lines) (= (.trim line) ""))
          nil
          (let [line-vector (read-string line)
                nt-line (get-nt-line namespaces filename line-vector)]
            (do (.write wrtr (str nt-line " .\n"))
                (recur (rest lines)))))))))

