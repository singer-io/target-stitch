(defproject persist-stitch "0.1.0-SNAPSHOT"
  :description "Persists Stitch Stream Encoded data from stdin to the Stitch Import API"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.6.3"]
                 [com.stitchdata/clojure-stitch-client "0.1.6"]]
  :uberjar-name "persist-stitch-standalone.jar"
  :main com.stitchdata.persist.stitch.core)
