(defproject clj-hadoop "0.1.0-SNAPSHOT"
  :description "hadoop stuff"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  ;;  :main hadoop.tools.run
  :aot [hadoop.tools.run hadoop.tools.mapper hadoop.tools.reducer]
  :uberjar-name "uber-hadoop.jar"  
  :dependencies [[org.clojure/clojure "1.5.0"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.1"]
               [org.apache.hadoop/hadoop-mapreduce-client-common "2.0.0-cdh4.1.1"]]}}
  :repositories [["java.net" "http://download.java.net/maven/2"]
                 ["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"]])


