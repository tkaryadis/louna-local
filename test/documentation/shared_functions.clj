(ns documentation.shared-functions)

(defn get-files-paths [path]
  (let [path (clojure.java.io/file path)
        files (file-seq path)]
    (reduce (fn [files-vec file]
              (if (.isFile file)
                (conj files-vec [(.getName file) (.getAbsolutePath file)])
                files-vec))
            []
            files)))

(defn get-file-extension [file]
  (let [index (.lastIndexOf file ".")]
    (if (= index -1)
      ""
      (subs file index (.length file)))))


