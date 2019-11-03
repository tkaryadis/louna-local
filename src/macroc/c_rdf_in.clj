(ns macroc.c-rdf-in
  (:require library.util
            [clojure.java.io :as io]
            state.state
            state.db-settings
            macroc.c-save)
  (:import (rdfconvert RDF2RDF)))

;;---------------------------------ttl to .nt-------------------------------------------------

;;convert rdf ,the type of conversion depends on the extension
(defn convert-rdf [source-path dest-path]
  (RDF2RDF/main (into-array [source-path dest-path])))

;;@prefix ab: <http://learningsparql.com/ns/addressbook#> .

(defn add-line-to-namespaces [namespaces line]
  (cond
    (clojure.string/starts-with? line "@prefix")
    (let [line-parts  (clojure.string/split line #"\s+")
          prefix (get line-parts 1)
          prefix (if (= prefix ":")
                   "empty"
                   (subs prefix 0  (dec (.length prefix))))
          uri (get line-parts 2)
          uri (subs uri 1 (dec (.length uri)))]
      [(assoc namespaces prefix uri) false])
    (clojure.string/starts-with? line "@base")
    [namespaces false]
    (clojure.string/starts-with? line "#")
    [namespaces false]
    (= line "")
    [namespaces false]
    :else
    [namespaces true]))

(defn ttl-to-nt-file [dir namespaces filename]
  (with-open [rdr (clojure.java.io/reader (str (state.db-settings/get-rdf-path) dir "/" filename))]
    (loop [lines (line-seq rdr)
           namespaces namespaces
           stop? false]
      (let [line (first lines)]
        (if (or (empty? lines) stop?)
          namespaces
          (let [[namespaces stop?] (add-line-to-namespaces namespaces line)]
            (recur (rest lines) namespaces stop?)))))))

(defn ttl-to-nt [dir]
  (let [directory (clojure.java.io/file (str (state.db-settings/get-rdf-path) dir "/"))
        ttl-files (filter #(.endsWith % ".ttl") (map #(.getName %) (file-seq directory)))]
    (if-not (empty? ttl-files)
      (let [- (dorun (map (fn [ttl-filename]
                            (let [nt-filename (clojure.string/replace ttl-filename #".ttl" "gen.nt")]
                              (convert-rdf (str (state.db-settings/get-rdf-path) dir "/" ttl-filename)
                                           (str (state.db-settings/get-rdf-path) dir "/" nt-filename))))
                          ttl-files))
            prv-namespaces (read-string (slurp (str (state.db-settings/get-rdf-path)
                                                    dir
                                                    "/ns")))

            ;;i dont care about namespace conflicts if prefix-conflict we may lose a prefix,data will not be effected
            namespaces (reduce (fn [namespaces filename]
                                 (ttl-to-nt-file dir namespaces filename ))
                               prv-namespaces
                               ttl-files)]
        (spit (clojure.java.io/file (str (state.db-settings/get-rdf-path) dir "/ns")) namespaces))
      nil)))

(defn rdf-to-ttl [dir]
  (let [directory (clojure.java.io/file (str (state.db-settings/get-rdf-path) dir "/"))
        rdf-files (filter #(.endsWith % ".rdf") (map #(.getName %) (file-seq directory)))]
    (dorun (map (fn [rdf-filename]
                  (let [ttl-filename (clojure.string/replace rdf-filename #".rdf" "gen.ttl")]
                    (convert-rdf (str (state.db-settings/get-rdf-path) dir "/" rdf-filename)
                                 (str (state.db-settings/get-rdf-path) dir "/" ttl-filename))))
                rdf-files))))

;;-----------------------------------------------------------------------------------------------------------------

;;URI -> prefix:name i _:name
;;literal
;;  number px 5
;;  string px "takis"

(defn add-namespace [namespaces namespace]
  (if-not (contains? namespaces namespace)
    (let [new-namespace-id (state.state/get-new-namespace-ID)]
      [(assoc namespaces namespace new-namespace-id) new-namespace-id])
    [namespaces (get namespaces namespace)]))

(defn seperate-namespace [namespaces token]
  (cond

    (clojure.string/ends-with? token "\"")                  ;;simple string
    [namespaces (subs  token 1 (dec (.length token)))]

    (clojure.string/starts-with? token "<mail")
    (let [token (.substring token 1 (- (.length token) 1))
          token (subs token 7 (.length token))]
      [namespaces token]
      ;[(assoc namespaces "mail" "mail:to") (keyword (str "mail" "." token))]
      )

    (clojure.string/starts-with? token "<")  ;;ean einai IRI
    (let [token (.substring token 1 (- (.length token) 1))    ;;petao ta <>
          token (if (clojure.string/ends-with? token "/") (.substring token 0 (- (.length token) 1)) token) ;;petao last /
          [namespace value] (library.util/get-namespace-value token)
          [namespaces namespace-value] (add-namespace namespaces namespace)]
      [namespaces (keyword (str namespace-value "." value))])

    (clojure.string/starts-with? token "_:") ;;ean einai blank_node
    [namespaces (keyword (str "_." (.substring token 2)))]

    (not= (.indexOf token "^^") -1)                         ;;type used
    (let [index (.indexOf token "^^")
          str-part (subs token 0 index)]
      (if (or (clojure.string/ends-with? token "integer>")
              (clojure.string/ends-with? token "long>")
              (clojure.string/ends-with? token "decimal>")
              (clojure.string/ends-with? token "boolean>")
              (clojure.string/ends-with? token "string>")
              (clojure.string/ends-with? token "byte>")
              (clojure.string/ends-with? token "short>"))
        [namespaces (read-string (subs  str-part 1 (dec (.length str-part))))]
        [namespaces (subs  str-part 1 (dec (.length str-part)))])) ;;unknown type,ignore type

    (not= (.indexOf token "\"@") -1)
    (let [index (.lastIndexOf token "@")
          token (subs token 1 (dec index))]
      [namespaces token])


    :else
    [namespaces (subs  token 1 (dec (.length token)))]   ;;simple string
    ))

(defn get-tokens-from-line [line]
  (let [index1 (.indexOf line " ")
        s (subs line 0 index1)
        line (subs line (+ index1 1))
        index2 (.indexOf line " ")
        r (subs line 0 index2)
        line (subs line (+ index2 1))
        o (.substring line 0 (- (.length line) 2))]
    [s r o]))

;;stin line theoro oti ta 2 prota(s r) einai pant namespaced
;;eno to value isos nai isos oxi
(defn add-line-to-constructs [db constructs line]
  (let [[s r o] (get-tokens-from-line line)
        [namespaces new-s] (seperate-namespace (get constructs :ns {}) s)
        [namespaces new-r] (seperate-namespace namespaces r)
        [namespaces new-o] (seperate-namespace namespaces o)
        constructs (macroc.c-save/inc-size db constructs)]
    (if (contains? constructs new-r)
      (-> constructs
          (assoc :ns namespaces)
          (update new-r conj [new-s new-o]))
      (-> constructs
          (assoc :ns namespaces)
          (assoc new-r [[new-s new-o]])))))


(defn read-store-rdf-file [db dir constructs filename]
  (with-open [rdr (clojure.java.io/reader (str (state.db-settings/get-rdf-path) dir "/" filename))]
    (loop [lines (line-seq rdr)
           constructs constructs]
      (let [line (first lines)]
        (if (or (empty? lines) (clojure.string/blank? line))
          constructs
          (recur (rest lines) (add-line-to-constructs db constructs line)))))))

(defn load-rdf-constructs [db constructs dir]
  (let [directory (clojure.java.io/file (str (state.db-settings/get-rdf-path) dir "/"))
        - (let [ns-exists? (.exists (io/as-file (str (state.db-settings/get-rdf-path) dir "/" "ns")))]
            (if-not ns-exists?
              (spit (str (state.db-settings/get-rdf-path) dir "/ns") "{}")))
        prv-gen-rdf-files (filter #(or (.endsWith % "gen.nt") (.endsWith % "gen.ttl")) (map #(.getName %) (file-seq directory)))
        - (dorun (map (fn [filename]
                        (clojure.java.io/delete-file (clojure.java.io/file (str (state.db-settings/get-rdf-path) dir "/" filename))))
                      prv-gen-rdf-files))
        - (rdf-to-ttl dir)
        - (ttl-to-nt dir)
        rdf-files (filter #(.endsWith % ".nt") (map #(.getName %) (file-seq directory)))
        pre-namespaces-inv (clojure.set/map-invert
                              (read-string (slurp (str (state.db-settings/get-rdf-path)
                                                      dir
                                                      "/ns"))))
        constructs (reduce (fn [constructs filename]
                             (read-store-rdf-file db dir constructs filename ))
                           (assoc constructs :ns pre-namespaces-inv)
                           rdf-files)
        constructs (assoc constructs :ns (clojure.set/map-invert (:ns constructs)))]
    constructs))