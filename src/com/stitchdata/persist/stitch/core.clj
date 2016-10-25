(ns com.stitchdata.persist.stitch.core
  (:require [cheshire.core :refer [generate-string parse-string]]
            [com.stitchdata.client.core :as sc])
  (:gen-class))

(def records-persisted (atom 0))

(defn parse-headers
  [in-reader]
  (let [protocol (first (line-seq in-reader))
        headers (doall (take-while (partial not= "--") (line-seq in-reader)))]
    (when (not= protocol "stitchstream/0.1")
      (throw (ex-info (str "Unsupported protocol: " protocol) {})))))

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
      (with-open [in-reader     (java.io.BufferedReader. *in*)
                  stitch-client (sc/client {::sc/client-id (Integer/parseInt stitch-client-id)
                                            ::sc/token stitch-token
                                            ::sc/namespace stitch-namespace})]
        (parse-headers in-reader)
        (doseq [ln (line-seq in-reader)]
          (let [parsed (parse-string ln)]
            (condp = (get parsed "type")

              "RECORD" (do
                         (sc/push stitch-client {::sc/action ::sc/upsert
                                                 ::sc/sequence (System/currentTimeMillis)
                                                 ::sc/table-name (get parsed "stream")
                                                 ::sc/key-names (get parsed "key_fields")
                                                 ::sc/data (get parsed "record")})
                         (swap! records-persisted inc))

              "BOOKMARK" (binding [*out* *err*]
                           (println "Bookmark:" (generate-string (get parsed "value")))))))))
    (binding [*out* *err*]
      (println "Persisted" @records-persisted "records"))))
