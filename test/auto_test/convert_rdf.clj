(ns auto-test.convert-rdf
  (:use louna.louna))

(def rdf-dirs ["ex002" "ex012" "ex041" "ex054" "ex069" "ex074" "ex100" "ex104" "ex122" "ex122-123"
               "ex138" "ex145" "ex187" "ex198" "sp1" "spValues1"])

(dorun (map (fn [rdf-dir] (c {:c-out [rdf-dir]}
                             (:rdf  rdf-dir)))
            rdf-dirs))

