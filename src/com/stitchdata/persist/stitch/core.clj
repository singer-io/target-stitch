(ns com.stitchdata.persist.stitch.core
  (:require [cheshire.core :refer [generate-string parse-string]]
            [com.stitchdata.client.core :as sc])
  (:gen-class))

(def ^:dynamic *current-block*)

(defn stream-name
  [block-header]
  (nth (re-find #"==[A-Z]+==for==(.+)" *current-block*) 1))

(defn -main [& args]
  (let [stitch-token (or (System/getenv "STITCH_TOKEN")
                         (throw (ex-info "STITCH_TOKEN not set" {})))
        stitch-namespace (or (System/getenv "STITCH_NAMESPACE")
                             (throw (ex-info "STITCH_NAMESPACE not set" {})))
        stitch-client-id (or (System/getenv "STITCH_CLIENT_ID")
                             (throw (ex-info "STITCH_CLIENT_ID not set" {})))]
    (binding [*out* *err*]
      (println "Persisting to Stitch"))
    (try
      (with-open [stitch-client (sc/client {::sc/client-id (Integer/parseInt stitch-client-id)
                                            ::sc/token stitch-token
                                            ::sc/namespace stitch-namespace})]
        (binding [*current-block* "null"]
          (doseq [ln (line-seq (java.io.BufferedReader. *in*))]
            (if (= "=" (subs ln 0 1))

              (do
                (set! *current-block* ln))

              (let [block-type (subs *current-block* 0 9)]
                (condp = block-type

                  "==RECORDS" (sc/push stitch-client {::sc/action ::sc/upsert
                                                      ::sc/sequence (System/currentTimeMillis)
                                                      ::sc/table-name (stream-name *current-block*)
                                                      ::sc/key-names ["sha"]
                                                      ::sc/data (parse-string ln)})

                  "==BOOKMAR"   (binding [*out* *err*]
                                  (println "Bookmark:" ln)))))))))))
