(ns hadoop.tools.mapper
  (:import [org.apache.hadoop.mapreduce Mapper MapContext])
  (:import [org.apache.hadoop.io Text LongWritable IntWritable])
  (:gen-class
   :extends org.apache.hadoop.mapreduce.Mapper))


(defn -map [this ^LongWritable key ^Text val ^MapContext context]
  (.write context
          (Text. (str (first (.toString val))))
          (IntWritable. (Integer. 1))))
