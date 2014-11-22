(ns hadoop.tools.run
  (:import [org.apache.hadoop.fs Path])
  (:import [org.apache.hadoop.mapreduce.lib.input FileInputFormat])
  (:import [org.apache.hadoop.mapreduce.lib.output FileOutputFormat])
  (:import [org.apache.hadoop.mapreduce Job JobContext])
  (:import [org.apache.hadoop.io Text LongWritable IntWritable])
  (:gen-class))

(defn- ^String as-str [s]
  (cond (keyword? s) (name s)
        (class? s) (.getName ^Class s)
        (fn? s)
        (throw (Exception. "Cannot use function as value; use a symbol."))
        :else (str s)))

(defn- ^Path as-path [s]
  (Path. s))

(defn makeJob [input output]
  (let [^Job job (Job.)]
    (doto job
      (.setJarByClass hadoop.tools.run)
      (.setMapperClass (Class/forName "hadoop.tools.mapper"))
      (.setReducerClass (Class/forName "hadoop.tools.reducer"))
      (.setOutputKeyClass Text)
      (.setOutputValueClass IntWritable)
      (FileInputFormat/setInputPaths ^Path (as-path input))      
      (FileOutputFormat/setOutputPath (Path. (as-str output))))))

(defn -main [input output & args]
  (println input)
  (println output)
  (let [job (makeJob input output)]
    (.waitForCompletion job true)))

