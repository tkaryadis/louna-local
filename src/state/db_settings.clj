(ns state.db-settings
  (:require [clojure.java.io :as io])
  (:import (java.io File)))

;;------------------base-path-----------------------------------------

(defn project-folder? [file]
  (let [path (.getAbsolutePath file)]
    (.exists (io/file (str path "/" "project.clj")))))

(defn get-project-folder [cwd]
  (loop [path-file (File. cwd)]
    (if (project-folder? path-file)
      (.getAbsolutePath path-file)
      (recur (.getParentFile path-file)))))

(def base-path (atom  (let [cwd (System/getProperty "user.dir")
                            project-folder (str (get-project-folder cwd) "/")
                            settings-path (str project-folder "db-settings")
                            file-base-path (.exists (io/file settings-path))
                            file-base-path (if file-base-path (get (read-string (slurp settings-path))
                                                                   :base-path)
                                                              false)
                            base-path (if (and file-base-path (not= file-base-path ""))
                                        (if (clojure.string/ends-with? file-base-path "/")
                                          file-base-path
                                          (str file-base-path "/"))
                                        (str project-folder "louna-data/"))
                            - (if-not (.exists (io/file (str base-path)))
                                (do (.mkdir (java.io.File. base-path))
                                    (.mkdir (java.io.File. (str base-path "/dbs")))
                                    (.mkdir (java.io.File. (str base-path "/rdf")))
                                    (.mkdir (java.io.File. (str base-path "/tables")))))]
                        base-path)))

;;------------------dbs-path-------------------------------------------

(defn get-dbs-path []
  (str @base-path "dbs/"))

;;-----------------rdf-path--------------------------------------------


(defn get-rdf-path []
  (str @base-path "rdf/"))

;;------------------tables-path----------------------------------------

(defn get-tables-path []
  (str @base-path "tables/"))

;;-----------------default-db-----------------------------------------
(def default-db (atom ""))

(defn set-default-db [db]
  (do (reset! default-db db)))

;;----------------join-method-----------------------------------------

(def join-method (atom "disk"))

(defn set-join-method [method]
  (reset! join-method method))

(defn get-join-method []
  @join-method)

;;---------------max construct size in memory-------------------------

(def max-construct-size 200000)
