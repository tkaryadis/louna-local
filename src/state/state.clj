(ns state.state
  (:require clojure.set
            state.db-settings
            [clojure.tools.analyzer.jvm :as ana.jvm]))


;;-------------------line-ids----------------------------------

(def line-ID (atom 0))
(defn get-new-line-ID []
  (swap! line-ID inc))

;;-------------------var-ids-----------------------------------

(def var-ID (atom 99))

(defn get-new-var-ID []
  (let [var-ID(swap! var-ID inc)]
    (str "?TMP" var-ID)))

;;------------------namespace-IDS------------------------------

(def namespace-ID (atom 0))
(defn get-new-namespace-ID []
  (str "p" (swap! namespace-ID inc)))

;;----------------values for eval------------------------------

(def cur-value-id (atom 0))

(defn get-new-value-id []
  (swap! cur-value-id inc))

(def value-map (atom {}))

(defn get-value [value-ID]
  (get @value-map value-ID))

(defn add-new-value [value-id value]
  (swap! value-map assoc value-id value))

(defn remove-value [value-ID]
  (swap! value-map dissoc value-ID))

;;---------------unresolved-variables--------------------------------------

(def code-ID (atom 0))

(defn get-new-code-ID []
  (swap! code-ID inc))


(def undefined (atom {}))

(defn save-unresolved [c-id _ s _]
  (let [s-str (str s)]
    (if-not (clojure.string/starts-with? s-str "?")
      (do (swap! undefined assoc c-id (conj (get @undefined c-id #{}) s-str)) {})
      {})))

(defn clear-undefined []
  (reset! undefined {}))

(defn get-undefined [c-id]
  (get @undefined c-id))

(defn remove-code [c-id]
  (swap! undefined dissoc c-id))

(defn add-code-locals [c-id locals]
  (swap! state.state/undefined assoc c-id locals))

(defn merge-locals [codes-locals]
  (reset! undefined (merge @undefined codes-locals)))

(defn analyse-query [c-id query]
  (ana.jvm/analyze
    query
    (ana.jvm/empty-env)
    {:passes-opts
     (assoc ana.jvm/default-passes-opts
       :validate/unresolvable-symbol-handler (partial save-unresolved c-id))}))


;(println @undefined)