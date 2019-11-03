(defproject louna/louna-local "0.1.0-SNAPSHOT"
  :description "Local data processing and quering using a SPARQL like DSL"
  :url "https://tkaryadis.github.io/louna-local"
  :license {:name "Apache License"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [
                 [org.clojure/clojure "1.9.0"]
                 ;;used for local symbols
                 [org.clojure/tools.analyzer.jvm "0.7.2"]
                 ;;match for the test examples
                 [org.clojure/core.match "0.3.0-alpha5"]
                 ;;jena for the benchmark
                 [org.apache.jena/jena-core "3.0.0"]
                 [org.apache.jena/jena-arq "3.0.0"]
                 [org.apache.jena/jena-tdb "3.0.0"]
                 ;;used for rdf convertion ttl to nt etc
                 [org.openrdf.sesame/sesame-rio-api "2.6.10"]
                 ]
  ;:main auto-test.auto-book-q
  :aot  [auto-test.auto-book-q]      ;;aot gia uberjar
  :java-source-paths ["src/java"]
  :plugins [[lein-codox "0.10.7"]]
  )
