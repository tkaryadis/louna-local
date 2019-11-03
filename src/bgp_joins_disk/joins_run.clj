(ns bgp-joins-disk.joins-run
  (:require bgp-joins-disk.join-filter-relations
            bgp-joins-disk.relations-run-init
            bgp-joins-disk.relations-run-join
            bgp-joins-disk.path-run-seq-star-plus
            bgp.binds-run
            state.state))

;;x= s-var i o-var
(defn add-line-1var [sorted-vars-add-var table line join-var-index relation-map binds table-filter-functions]
  (let [join-var-line-value (if (coll? join-var-index)
                               [(get line (get join-var-index 0)) (get line (get join-var-index 1))]
                               (get line join-var-index))
        add-var (peek sorted-vars-add-var)]
    (if (contains? relation-map join-var-line-value)
      (let [njoin-var-relation-value (get relation-map join-var-line-value)]
        (if (coll? njoin-var-relation-value)
          (loop [table table
                 index 0]
            (if (= index (count njoin-var-relation-value))
              table
              (let [line-to-add (conj line (get njoin-var-relation-value index))
                    passed? (bgp.filters-run/run-table-filter-vectors add-var table-filter-functions sorted-vars-add-var line-to-add)
                    line-to-add (if passed? (bgp.binds-run/add-binds-to-line sorted-vars-add-var binds table-filter-functions line-to-add) [])]
                (if-not (empty? line-to-add)
                  (recur (assoc table (state.state/get-new-line-ID) line-to-add) (inc index))
                  (recur table (inc index))))))
          (let [line-to-add (conj line njoin-var-relation-value)
                passed? (bgp.filters-run/run-table-filter-vectors add-var table-filter-functions sorted-vars-add-var line-to-add)
                line-to-add (if passed? (bgp.binds-run/add-binds-to-line sorted-vars-add-var binds table-filter-functions line-to-add) [])]
            (if-not (empty? line-to-add)
              (assoc table (state.state/get-new-line-ID) line-to-add)
              table))))
      table)))

(defn add-line-2vars [sorted-vars-add-vars table line join-var-index relations-map binds table-filter-functions]
  (let [s-line-value (get line join-var-index)
        x-var (peek sorted-vars-add-vars)]
    (if (contains? relations-map s-line-value)
      (let [rx-relation-values (get relations-map s-line-value)]
        (loop [table table
               index 0]
          (if (= index (count rx-relation-values))
            table
            (let [rx-relation-value (get rx-relation-values index)
                  line-to-add (apply (partial conj line) rx-relation-value)
                  passed? (bgp.filters-run/run-table-filter-vectors x-var table-filter-functions sorted-vars-add-vars line-to-add)
                  line-to-add (if passed? (bgp.binds-run/add-binds-to-line sorted-vars-add-vars binds table-filter-functions line-to-add) [])]
              (if-not (empty? line-to-add)
                (recur (assoc table (state.state/get-new-line-ID) line-to-add) (inc index))
                (recur table (inc index)))))))
      table)))

(defn join-relation-map-table [sorted-vars table join-vars add-vars r-var s-var o-var relation-map binds table-filter-functions]
  (let [join-var-index (cond
                         (= join-vars #{s-var})
                         (library.util/get-var-position sorted-vars s-var)
                         (= join-vars #{o-var})
                         (library.util/get-var-position sorted-vars o-var)
                         (= join-vars #{r-var})
                         (library.util/get-var-position sorted-vars r-var)
                         (= join-vars #{s-var o-var})
                         [(library.util/get-var-position sorted-vars s-var)
                          (library.util/get-var-position sorted-vars o-var)]
                         (= join-vars #{r-var s-var})
                         [(library.util/get-var-position sorted-vars r-var)
                          (library.util/get-var-position sorted-vars s-var)]
                         (= join-vars #{r-var o-var})
                         [(library.util/get-var-position sorted-vars r-var)
                          (library.util/get-var-position sorted-vars o-var)]
                         :else (do (println "Unknown join vars") (System/exit 0)))
        sorted-vars-add-vars (apply (partial conj sorted-vars) add-vars)]
    (loop [table-keys (keys table)
           table table]
      (if (empty? table-keys)
        table
        (let [cur-key (first table-keys)
              line (get table cur-key)
              table (dissoc table cur-key)
              new-table (cond
                          (= (count add-vars) 1)
                          (add-line-1var sorted-vars-add-vars
                                         table
                                         line
                                         join-var-index
                                         relation-map
                                         binds
                                         table-filter-functions)
                          (= (count add-vars)  2)
                          (add-line-2vars  sorted-vars-add-vars
                                           table
                                           line
                                           join-var-index
                                           relation-map
                                           binds
                                           table-filter-functions)
                          :else (do (println "Unknown join-vars" add-vars) (System/exit 0)))]
          (recur (rest table-keys) new-table))))))

;;var domains kratao mono gia tin epomeni join var
(defn make-joins [q-info table-info]
  (let [relations (get table-info "relations")
        table-filter-functions (get table-info "table-filter-functions")]
    (loop [index 1
           sorted-vars (get table-info "sorted-vars")
           table (get table-info "table")]
      (if (= index (count relations))
        (-> table-info
          (assoc "sorted-vars" sorted-vars)
          (assoc "table" table))
        ;;epidi den kano cartesian kai panta bazo metabliti(alios ti ixa kani filtro),
        ;;panta exo join-vars kai panta exo add-vars
        ;;ara panta edo tha exo relation me toulaxiston 2 vars
        ;;ara ite 1 + 1 (so/rs/ro ite  rso me 1+2 ite 2+1)
        ;;ara periptosis  so/rso/rs/ro
        (let [relation (get relations index)
              rel-type (get relation "rel-type")
              join-vars (get relation "join-vars")
              property (get relation "property")]
          (cond
            (= rel-type "so")    ;;so me join var tin s i tin o   ;;;;theli elenxo ean property
            (let [[s-var o-var] (get relation "pair")
                  join-var (first join-vars)
                  [map-key add-vars]  (if (= join-var s-var) ["s" [o-var]]  ["o" [s-var]])
                  relation-map (cond
                                 (nil? property)
                                 (bgp-joins-disk.relations-run-join/triple-join-so q-info relation map-key)
                                 (= (get property "prop-type") "alt")
                                 (bgp-joins-disk.relations-run-join/triple-join-rso q-info relation map-key true)
                                 (= (get property "prop-type") "seq+")
                                 (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-info relation 2 map-key false)
                                 (= (get property "prop-type") "seq*")
                                 (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-info relation 2 map-key true))
                  binds (get relation "binds" [])
                  bind-vars (reduce (fn [bind-vars bind] (conj bind-vars (get bind "bind-var"))) [] binds)
                  new-table (join-relation-map-table sorted-vars table join-vars add-vars nil s-var o-var relation-map binds table-filter-functions)
                  sorted-vars-binds (into [] (concat sorted-vars add-vars bind-vars))
                  new-table (bgp-joins-disk.join-filter-relations/apply-filter-relations q-info sorted-vars-binds new-table (get relation "filter-relations" []))]
              (recur (inc index)
                     sorted-vars-binds
                     new-table))
            (= rel-type "rs")
            (let [[r-var s-var] (get relation "triple")
                  [map-key add-vars]  (if (= join-vars #{s-var}) ["s" [r-var]]  ["r" [s-var]])
                  relation-map (bgp-joins-disk.relations-run-join/triple-join-rs q-info relation map-key)
                  binds (get relation "binds" [])
                  bind-vars (reduce (fn [bind-vars bind] (conj bind-vars (get bind "bind-var"))) [] binds)
                  sorted-vars-binds (into [] (concat sorted-vars add-vars bind-vars))
                  new-table (join-relation-map-table sorted-vars table join-vars add-vars r-var s-var nil relation-map binds table-filter-functions)
                  new-table (bgp-joins-disk.join-filter-relations/apply-filter-relations q-info sorted-vars-binds new-table (get relation "filter-relations" []))]
              (recur (inc index)
                     sorted-vars-binds
                     new-table))
            (= rel-type "ro")
            (let [[r-var o-var] (get relation "triple")
                  [map-key add-vars]  (if (= join-vars #{o-var}) ["o" [r-var]]  ["r" [o-var]])
                  relation-map (bgp-joins-disk.relations-run-join/triple-join-ro q-info relation map-key)
                  binds (get relation "binds" [])
                  bind-vars (reduce (fn [bind-vars bind] (conj bind-vars (get bind "bind-var"))) [] binds)
                  sorted-vars-binds (into [] (concat sorted-vars add-vars bind-vars))
                  new-table (join-relation-map-table sorted-vars table join-vars add-vars r-var nil o-var relation-map binds table-filter-functions)
                  new-table (bgp-joins-disk.join-filter-relations/apply-filter-relations q-info sorted-vars-binds new-table (get relation "filter-relations" []))]
              (recur (inc index)
                     sorted-vars-binds
                     new-table))
            (= rel-type "rso")
            (let [[r-var s-var o-var] (get relation "triple")
                  [map-key add-vars] (cond (= join-vars #{s-var})
                                           ["s" [r-var o-var]]
                                           (= join-vars #{o-var})
                                           ["o" [r-var s-var]]
                                           (= join-vars #{r-var})
                                           ["r" [s-var o-var]]
                                           (= join-vars #{r-var s-var})
                                           ["rs" [o-var]]
                                           (= join-vars #{r-var o-var})
                                           ["ro" [s-var]]
                                           (= join-vars #{s-var o-var})
                                           ["so" [r-var]])
                  relation-map (bgp-joins-disk.relations-run-join/triple-join-rso q-info relation map-key false)
                  binds (get relation "binds" [])
                  bind-vars (reduce (fn [bind-vars bind] (conj bind-vars (get bind "bind-var"))) [] binds)
                  sorted-vars-binds (into [] (concat sorted-vars add-vars bind-vars))
                  new-table (join-relation-map-table sorted-vars table join-vars add-vars r-var s-var o-var relation-map binds table-filter-functions)
                  new-table (bgp-joins-disk.join-filter-relations/apply-filter-relations q-info sorted-vars-binds new-table (get relation "filter-relations" []))]
              (recur (inc index)
                     sorted-vars-binds
                     new-table))))))))

(defn init-table [q-info table-info]
  (let [relations (get table-info "relations")
        first-relation (get relations 0)
        binds (get first-relation "binds")
        bind-vars (map (fn [b] (get b "bind-var")) binds)
        rel-type (get first-relation "rel-type")
        table-filter-functions (get table-info "table-filter-functions")

        sorted-vars (cond
                    (= rel-type "s") [(get first-relation "s-var")]
                    (= rel-type "o") [(get first-relation "o-var")]
                    (= rel-type "so") (get first-relation "pair")
                    (or (= rel-type "rso")
                        (= rel-type "rs")
                        (= rel-type "ro")
                        (= rel-type "r")) (get first-relation "triple"))

        binds-f (partial bgp.binds-run/add-binds-to-line sorted-vars binds table-filter-functions)

        table (cond
                (= rel-type "s")
                (let [property (get first-relation "property")]
                  (if (nil? property)
                    (bgp-joins-disk.relations-run-init/triple-init-s q-info first-relation binds-f)
                    (cond
                      (= (get property "prop-type") "alt")
                      (bgp-joins-disk.relations-run-init/triple-init-rs q-info first-relation binds-f true)
                      (= (get property "prop-type") "seq+")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-info first-relation binds-f true false)
                      (= (get property "prop-type") "seq*")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-info first-relation binds-f true true))))
                (= rel-type "o")
                (let [property (get first-relation "property")]
                  (if (nil? property)
                    (bgp-joins-disk.relations-run-init/triple-init-o q-info first-relation binds-f)
                    (cond
                      (= (get property "prop-type") "alt")
                      (bgp-joins-disk.relations-run-init/triple-init-ro q-info first-relation binds-f true)
                      (= (get property "prop-type") "seq+")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-info first-relation binds-f false false)
                      (= (get property "prop-type") "seq*")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-s-or-o-seq-star-plus q-info first-relation binds-f false true))))
                (= rel-type "so")
                (let [property (get first-relation "property")]
                  (if (nil? property)
                    (bgp-joins-disk.relations-run-init/triple-init-so q-info first-relation binds-f)
                    (cond
                      (= (get property "prop-type") "alt")
                      (bgp-joins-disk.relations-run-init/triple-init-rso q-info first-relation binds-f true)
                      (= (get property "prop-type") "seq+")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-info first-relation 0 nil false)
                      (= (get property "prop-type") "seq*")
                      (bgp-joins-disk.path-run-seq-star-plus/triple-so-seq-star-plus q-info first-relation 0 nil true))))
                (= rel-type "r")   (bgp-joins-disk.relations-run-init/triple-init-r q-info first-relation)
                (= rel-type "rs")  (bgp-joins-disk.relations-run-init/triple-init-rs q-info first-relation binds-f false)
                (= rel-type "ro")  (bgp-joins-disk.relations-run-init/triple-init-ro q-info first-relation binds-f false)
                (= rel-type "rso") (bgp-joins-disk.relations-run-init/triple-init-rso q-info first-relation binds-f false))
        sorted-vars (into [] (concat sorted-vars bind-vars))
        table (bgp-joins-disk.join-filter-relations/apply-filter-relations q-info sorted-vars table (get first-relation "filter-relations" []))
        table-info (assoc table-info "sorted-vars" sorted-vars)
        table-info (assoc table-info "table" table)]
    table-info))