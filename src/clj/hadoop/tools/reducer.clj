(ns hadoop.tools.reducer
  (:import [org.apache.hadoop.mapreduce Reducer ReduceContext])
  (:import [org.apache.hadoop.io Text IntWritable])
  (:gen-class
   :extends org.apache.hadoop.mapreduce.Reducer))


(defn -reduce [this ^Text key ^Iterable values ^ReduceContext context]
  (let [sum (reduce
             #(+ %1 (.get %2)) 0
                    (iterator-seq
                     (.iterator values)))]
    (.write context key
            (IntWritable. (Integer/parseInt (.toString sum))))))