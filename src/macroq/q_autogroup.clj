(ns macroq.q-autogroup)

;;to bgp mpori na periexi asindeta pragmata pou theloun kartesian
;;episis meta to cartesian mpori na exo cartesian binds kai cartesian filters

;;px ena bgp mpori na einai 3 bgps
;;me binds-filtra sto 1-2, kai me bind-filters sto 1-2 result 3
;;arxika tha spaso to BGP se enomena tmimata
;;se kathe enomeno tmima tha balo ta filters/binds pou tou anikoun

;;oti mini tha do apo pia BGP exartate kai tha to balo san bind-filter group amesos meta
;;tora i seira pou tha kano tis BGP praxis den me niazi

;;tha balo ta bgp's se ena map me key tis vars tous
;;otan ena filter exi dep-vars pou iparxoun sto map tote mpeni stin omada
;;antistixa gia to bind,mono pou to bind bazi episis stin omada(ta binds exoun mpi me sosti sira are arkei 1 perasma)
(defn add-relational-queries-to-bgps [queries]
  (loop [queries queries
         rel-bgps {}
         bind-filters []
         prv-s nil]
    (if (empty? queries)
      [rel-bgps bind-filters]
      (let [query (first queries)]
        (if (= 1 (first query))
          (let [rest-query (rest query)
                new-prv-s (if (= (count rest-query) 3) (second rest-query) prv-s)
                rest-query (if (= (count rest-query) 2) (into [] (list (first rest-query) prv-s (nth rest-query 1))) rest-query)
                query (into [] (cons (first query) rest-query))
                query-vars (into #{} (library.util/get-vars rest-query))]
            (if (contains? rel-bgps query-vars)
              (let [prv-bgp (get rel-bgps query-vars)
                    new-bgp (conj prv-bgp query)]
                (recur (rest queries) (assoc rel-bgps query-vars new-bgp) bind-filters new-prv-s))
              (recur (rest queries) (assoc rel-bgps query-vars [query]) bind-filters new-prv-s)))
          (recur (rest queries) rel-bgps (conj bind-filters query) prv-s))))))

(defn add-bind-query-to-bgps [query bgps filter-cartesian]
  (let [dep-vars (library.util/get-vars-query-str (second (rest query)))
        add-var (first (rest query))]
    (loop [bgps-keys (keys bgps)
           bgps bgps
           added? false]
      (if (or (empty? bgps-keys) added?)
        (if added?
          [bgps filter-cartesian]
          [bgps (conj filter-cartesian query)])
        (let [bgp-vars (first bgps-keys)]
          (if (clojure.set/subset? dep-vars bgp-vars)
            (let [new-bgp-key (clojure.set/union bgp-vars #{add-var})
                  prv-bgp-value (get bgps bgp-vars)
                  new-bgp-value (conj prv-bgp-value query)]
              (recur (rest bgps-keys) (assoc (dissoc bgps bgp-vars) new-bgp-key new-bgp-value) true))
            (recur (rest bgps-keys) bgps false)))))))

(defn add-bind-queries-to-bgps [bgps queries]
  (loop [queries queries
         bgps bgps
         filter-cartesian []]
    (if (empty? queries)
      [bgps filter-cartesian]
      (let [query (first queries)]
        (if (= 2 (first query))
          (let [[bgps filter-cartesian] (add-bind-query-to-bgps query bgps filter-cartesian)]
            (recur (rest queries) bgps filter-cartesian))
          (recur (rest queries) bgps (conj filter-cartesian query)))))))

(defn add-filter-query-to-bgps [query bgps cartesian]
  (let [dep-vars (library.util/get-vars-query-str (first (rest query)))]
    (loop [bgps-keys (keys bgps)
           bgps bgps
           added? false]
      (if (or (empty? bgps-keys) added?)
        (if added?
          [bgps cartesian]
          [bgps (conj cartesian query)])
        (let [group-vars (first bgps-keys)]
          (if (clojure.set/subset? dep-vars group-vars)
            (let [prv-bgp (get bgps group-vars)
                  new-bgp (conj prv-bgp query)]
              (recur (rest bgps-keys) (assoc (dissoc bgps group-vars) group-vars new-bgp) true))
            (recur (rest bgps-keys) bgps false)))))))

(defn add-filter-queries-to-bgps [rel-bgps queries]
  (loop [queries queries
         [rel-bgps cartesian] [rel-bgps []]]
    (if (empty? queries)
      [rel-bgps cartesian]
      (let [query (first queries)]
        (if (= 3 (first query))
          (recur (rest queries) (add-filter-query-to-bgps query rel-bgps cartesian))
          (recur (rest queries) [rel-bgps (conj cartesian query)]))))))

(defn combine-bgp [bgp-key bgps combined-bgps]
  (let [bgp-value (get bgps bgp-key)
        bgps (dissoc bgps bgp-key)]
    (loop [bgps-keys (keys bgps)
           combine-key nil]
      (if (or (empty? bgps-keys) (not (nil? combine-key)))
        (if (nil? combine-key)
          [bgps (assoc combined-bgps bgp-key bgp-value)]
          (let [value2 (get bgps combine-key)
                new-key (clojure.set/union bgp-key combine-key)
                new-value (into [] (concat bgp-value value2))
                bgps (dissoc bgps combine-key)]
            [(assoc bgps new-key new-value) combined-bgps]))
        (let [cur-key (first bgps-keys)]
          (if (empty? (clojure.set/intersection bgp-key cur-key))
            (recur (rest bgps-keys) nil)
            (recur (rest bgps-keys) cur-key)))))))

;;perno to proto ean bro kapou na to balo tote telos(reduced)
;;ean den mpeni pouthena tote to bazo sta done-maps
;;stamatao ta balo ola (ite se kapio alo map eite sta done-maps)
(defn combine-bgps [bgps]
  (loop [bgps bgps
         combined-bgps {}]
    (if (empty? bgps)
      combined-bgps
      (let [bgp-key (first (first bgps))
            [bgps combined-bgps] (combine-bgp bgp-key bgps combined-bgps)]
        (recur bgps combined-bgps)))))

;;to kathe bgp tha gini  ("__and__" query1 query2)
;;to cartesian tha gini ("__fb__" query1 query2)
;;telika (("__and__" query1 query2) ("__and__" query1 query2) ("__fb__" query1 query2))
(defn add-operators-bgps [bgps]
  (let [bgps-vals (vals bgps)]
    (reduce (fn [bgps bgp] (conj bgps (into [] (concat (list "__and__") bgp))))
            []
            bgps-vals)))

;;theli beltiosi gia ta binds/filters ola ginonte sto telos
(defn get-groups [queries]
  (let [[bgps binds-filters-cartesian] (add-relational-queries-to-bgps queries)
        bgps (combine-bgps bgps)
        [bgps filters-cartesian] (add-bind-queries-to-bgps bgps binds-filters-cartesian)
        [bgps cartesian] (add-filter-queries-to-bgps bgps filters-cartesian)
        bgps (add-operators-bgps bgps)
        bgps (if (empty? cartesian) bgps (conj bgps (into [] (concat (list "__fb__") cartesian))))
        ;- (prn "bgps" bgps)
        ]
    bgps))