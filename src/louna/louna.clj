(ns louna.louna
  (:require library.util
            bgp.bgp-run
            groups.group-run
            library.print-data
            bgp.bgp-process
            macroq.q-process
            macroq.q-rewrite
            macroq.q-autogroup
            macroc.c-process
            macroc.c-run
            macroc.c-save))


;;--------------------------Construct-run----------------------------------
;;-------------------------------------------------------------------------


(defn run-c [undefined settings constructs-types table-info]
  (let [- (state.state/merge-locals undefined)

        c-in (get settings :c-in [])

        c-out (get settings :c-out [])
        c-out-set (into #{} c-out)
        print? (contains? c-out-set "print")
        db (first (disj c-out-set "print"))

        constant-cs (get constructs-types "constant-cs")
        relation-cs (get constructs-types "relation-cs")
        bind-cs (get constructs-types "bind-cs")
        rdf-cs (get constructs-types "rdf-cs")

        constant-construct-maps (map macroc.c-run/get-construct-map constant-cs)
        relation-construct-maps (map macroc.c-run/get-construct-map relation-cs)

        constructs {:size 0}
        constructs (macroc.c-run/add-rdf-constructs db constructs rdf-cs)
        constructs (macroc.c-run/add-relation-constructs db relation-construct-maps constructs table-info)
        constructs (macroc.c-run/add-constant-constructs db constructs constant-construct-maps)
        constructs (macroc.c-run/add-bind-constructs db constructs bind-cs)


        constructs (reduce (fn [constructs cur-table]
                             (macroc.c-run/add-relation-constructs db relation-construct-maps constructs cur-table))
                           constructs
                           c-in)  ;;extra tables

        - (if print?
            (library.print-data/print-constructs constructs))
        - (if-not (nil? db)
            (macroc.c-save/save-c db constructs))]
    constructs))

;;-----------------------------Query-run------------------------------------------------------------
;;------------------------------------------------------------------------------------------------------

(defn run-q [undefined settings group constructs]
  (let [- (state.state/merge-locals undefined)
        db (macroq.q-process/get-db settings)
        q-in-constructs (macroq.q-process/get-c-in-constructs settings)
        constructs (louna.louna-util/merge-relations q-in-constructs constructs)
        q-out (get settings :q-out [])
        file-out (first (filter #(not= % "print") q-out))
        [f-rel m-rel] (bgp-joins-disk.relations-run-shared/get-relations db constructs)
        q-info (macroq.q-process/get-q-info db constructs (into #{} f-rel) (into #{} m-rel))
        table-info (groups.group-run/run-group q-info group)
        print? (contains? (into #{} q-out) "print")
        - (if print? (library.print-data/print-table-stdout table-info))
        - (if-not (nil? file-out)
            (library.print-data/print-table-to-file
              (get table-info "sorted-vars")
              (get table-info "table") file-out))]
    table-info))

;;---------------------------macroq macro--------------------------------------------
;;--------------------------------------------------------------------------------

(defmacro q [& queries]
  (let [- (state.state/clear-undefined)
        [settings queries] (macroq.q-process/get-settings queries)
        [project queries] (macroq.q-process/get-project queries)

        group (into [] queries)

        ;;ean exi cmacro call
        last-query (last group)
        cmacro-call? (= (first last-query) 'c)
        [cmacro-call group] (if cmacro-call?
                            [(concat (list 'c) (rest last-query)) (pop group)]
                            [{} group])


        [group limit] (if (= (first (last group)) :limit) [(pop group) (second (last group))] [group -1])
        project-operation (if (= limit -1) [:project project] [:project project limit])
        group (conj group project-operation)
        group-str (macroq.q-process/add-str-group group)
        ;- (prn "group-str" group-str)
        q-out (list 'run-q
                    @state.state/undefined
                    settings
                    group-str
                    cmacro-call)
        ;- (prn "q-out" q-out)

        ]
    q-out))

;;--------------------------------c-macro-------------------------------------------------
;;----------------------------------------------------------------------------------------

(defmacro c [& constructs]
  (let [[settings constructs] (macroq.q-process/get-settings constructs)
        constructs (into [] constructs)
        last-construct (last constructs)
        qmacro-call? (= (first last-construct) 'q)
        [qmacro-call constructs] (if qmacro-call?
                                   [(concat (list 'q settings) (rest last-construct)) (pop constructs)]
                                   [{} constructs])
        constructs-types (macroc.c-process/get-constructs-types constructs)
        c-out (list 'run-c @state.state/undefined settings constructs-types qmacro-call)
        ;- (prn "c-out" c-out)
        ]
    c-out))