(ns bgp.bgp-run
  (:require clojure.set
            library.util
            bgp.filters-process
            bgp.relations-process
            bgp.binds-process
            bgp.joins-process
            library.print-data
            bgp.functions-process
            bgp-joins-disk.joins-run
            bgp-joins.joins-run))

;;BGP is always connected =>not cartesian
(defn run-bgp [q-info relation-queries bind-queries filter-queries]
  ;(prn "relation-queries" relation-queries)
  ;(prn "bind-queries" bind-queries)
  ;(prn "filter-queries" filter-queries)
  (let [;;relation-filters(relations/qrelations)
        ;;key=#{dep-vars}

        ;;join-filters
        ;;key=add-var

        ;;relation-filters will be embeded to queries/binds

        table-info {"relation-queries" relation-queries
                    "bind-queries"     bind-queries
                    "filter-queries"   filter-queries}


        ;;{"relation-queries" relation-queries
        ;; "bind-queries" bind-queries
        ;; "filter-queries" filter-queries


        ;; "pairs"
        ;; "triples"
        ;; "vars"
        ;; "bind-vars" bind-vars}

        table-info (bgp.relations-process/add-vars-info table-info)


        ;;{"relation-queries" relation-queries
        ;; "bind-queries" bind-queries

        ;;"vars-filter-functions"
        ;;"table-filter-queries"

        ;; "vars" vars
        ;; "bind-vars" bind-vars}
        table-info (bgp.filters-process/seperate-filters table-info)


        ;;{"relation-queries" relation-queries
        ;; "binds" binds

        ;;"vars-filter-functions"
        ;;"table-filter-queries"

        ;; "vars" vars
        ;; "bind-vars" bind-vars}
        table-info (bgp.binds-process/add-binds table-info)

        ;;relation            = {"query" query
        ;;                       "type" "s/o/so/rso/rs/ro/r"
        ;;                       "relation" relation
        ;;                       "s-var/o-var/pair/triple"
        ;;                       "filters" {}
        ;;                       "vars" #{}}


        ;;{ "relations" relations
        ;;  "binds" binds
        ;;  "table-filter-queries" []}
        table-info (bgp.relations-process/relations-queries-to-relations table-info)

        table-info (bgp.relations-process/add-cost-to-relations q-info table-info)


        ;;{ "relations" relations  ;;ordered relations
        ;;  "sorted-vars" sorted-vars

        ;;  "binds" binds

        ;;  "table-filter-queries" [] }
        table-info (bgp.joins-process/order-queries table-info)

        ;;{ "relations" relations  ;;ordered relations
        ;;  "sorted-vars" sorted-vars  ;;exoun kai ta binds pleon

        ;;  "binds" binds

        ;;  "table-filter-queries" []}
        table-info (bgp.binds-process/add-bind-vars-to-sorted-vars table-info)

        ;;{ "relations" relations  ;;ordered relations
        ;;  "sorted-vars" sorted-vars  ;;exoun kai ta binds pleon

        ;;  "binds" binds   ;;bind-map pleon,me key last table-dep

        ;;  "table-filter-queries" []}

        table-info (bgp.binds-process/add-trigger-var-to-binds table-info)

        ;;{ "relations" relations  ;;ordered relations with BINDS
        ;;  "sorted-vars" sorted-vars  ;;exoun kai ta binds pleon

        ;;  "table-filter-queries" []}
        table-info (bgp.binds-process/add-binds-to-relations table-info)

        ;;{"relations" relations    ;;relations exoun san meli ta binds tous
        ;; "table-filter-functions" {}}
        table-info (bgp.filters-process/add-trigger-var-to-table-filters table-info)


        ;- (library.print-data/print-query-plan (get table-info "relations") true)
        ;- (prn (get table-info "table-filter-functions"))

        ;- (prn (get table-info "relations"))

        ;;join-method-pick
        table-info (if (= (state.db-settings/get-join-method) "disk")
                       (let [table-info (bgp-joins-disk.joins-run/init-table q-info table-info)
                             table-info  (bgp-joins-disk.joins-run/make-joins q-info table-info)] ;;bgp-joins-disk
                         table-info)
                       (let [table-info  (bgp-joins.joins-run/make-joins q-info table-info)] ;;bgp-joins
                         table-info))

        table-info (assoc {} "sorted-vars" (get table-info "sorted-vars") "table" (get table-info "table"))
        ;- (prn "table" table-info)
        ]
    table-info))