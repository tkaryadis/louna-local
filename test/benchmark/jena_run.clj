(ns benchmark.jena-run
  (:import org.apache.jena.query.QueryExecutionFactory
           org.apache.jena.query.QueryFactory
           org.apache.jena.util.FileManager
           org.apache.jena.query.Syntax
           org.apache.jena.tdb.TDBFactory ))

(defn store-rdf [rdf-path]
  (let [dir (str (state.db-settings/get-dbs-path) "tdb")
        dataset (TDBFactory/createDataset dir)
        model (.getDefaultModel dataset)
        f (FileManager/get)
        -  (.readModel f model rdf-path)
        - (.close model)
        - (.close dataset)]))

(defn run-jena-tdb [queryString sorted-vars]
  (let [dir (str (state.db-settings/get-dbs-path) "tdb")
        dataset (TDBFactory/createDataset dir)
        queryString (str queryString)
        query (QueryFactory/create queryString Syntax/syntaxARQ)
        qexec  (QueryExecutionFactory/create query dataset)
        results (.execSelect qexec)
        - (println sorted-vars)]
    (loop [results results
           counter 0]
      (if-not (.hasNext results)
        nil
        (let [soln (.nextSolution results)
              - (dorun (map (fn [var-string]
                              (let [var-value (.get soln var-string)]
                                (print (.toString var-value) " ")))
                            sorted-vars))
              - (println)]
          (recur results (inc counter)))))))

(defn run-jena [queryString rdf-path sorted-vars]
  (let [f (FileManager/get)
        rdf-path (str rdf-path)
        model  (.loadModel f rdf-path)
        queryString (str queryString)
        query (QueryFactory/create queryString Syntax/syntaxARQ)
        qexec  (QueryExecutionFactory/create query model)
        results (.execSelect qexec)
        - (println sorted-vars)]
    (loop [results results
           counter 0]
      (if-not (.hasNext results)
        nil
        (let [soln (.nextSolution results)
              - (dorun (map (fn [var-string]
                              (let [var-value (.get soln var-string)]
                                (print (.toString var-value) " ")))
                            sorted-vars))
              - (println)]
          (recur results (inc counter)))))))