(ns com.stitchdata.persist.stitch.core
  (:require [cheshire.core :refer [generate-string parse-string]]
            [com.stitchdata.client.core :as sc])
  (:gen-class))

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
        (doseq [ln (line-seq (java.io.BufferedReader. *in*))]
          (let [parsed (parse-string ln)]
            (condp = (get parsed "type")

              "RECORDS" (doseq [record (get parsed "records")]
                          (sc/push stitch-client {::sc/action ::sc/upsert
                                                  ::sc/sequence (System/currentTimeMillis)
                                                  ::sc/table-name (get parsed "stream")
                                                  ::sc/key-names (get parsed "key_fields")
                                                  ::sc/data record}))

              "BOOKMARK" (binding [*out* *err*]
                           (println "Bookmark:" (generate-string (get parsed "value")))))))))))
