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



(comment

  {:bases #{org.apache.hadoop.mapreduce.InputFormat},
 :flags #{:public :abstract},
 :members #{#clojure.reflect.Method{:name getInputPaths,
                                    :return-type org.apache.hadoop.fs.Path<>,
                      
              :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat,
                                    :parameter-types [org.apache.hadoop.mapreduce.JobContext],
                                    :exception-types [],
                                    :flags #{:static :public}}
            #clojure.reflect.Method{:name listStatus, :return-type java.util.List, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext], :exception-types [java.io.IOException], :flags #{:protected}}
            #clojure.reflect.Constructor{:name org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [], :exception-types [], :flags #{:public}}
            #clojure.reflect.Field{:name PATHFILTER_CLASS, :type java.lang.String, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:static :public :final}}
            
            #clojure.reflect.Method{:name addInputPath, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job org.apache.hadoop.fs.Path], :exception-types [java.io.IOException], :flags #{:static :public}}
            
            #clojure.reflect.Method{:name computeSplitSize, :return-type long, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [long long long], :exception-types [], :flags #{:protected}}
            
            #clojure.reflect.Method{:name getMinSplitSize, :return-type long, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext], :exception-types [], :flags #{:static :public}}

            #clojure.reflect.Method{:name setInputPaths, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat,
                                    :parameter-types [org.apache.hadoop.mapreduce.Job java.lang.String], :exception-types [java.io.IOException], :flags #{:static :public}}
            #clojure.reflect.Method{:name setMinInputSplitSize, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job long], :exception-types [], :flags #{:static :public}}
            #clojure.reflect.Method{:name getBlockIndex, :return-type int, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.fs.BlockLocation<> long], :exception-types [], :flags #{:protected}}
            #clojure.reflect.Method{:name getPathStrings, :return-type java.lang.String<>, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [java.lang.String], :exception-types [], :flags #{:private :static}}
            
            #clojure.reflect.Method{:name addInputPaths, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job java.lang.String], :exception-types [java.io.IOException], :flags #{:static :public}}
            
            #clojure.reflect.Field{:name NUM_INPUT_FILES, :type java.lang.String, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:static :public :final}}
            #clojure.reflect.Method{:name getFormatMinSplitSize, :return-type long, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [], :exception-types [], :flags #{:protected}}
            #clojure.reflect.Method{:name setMaxInputSplitSize, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job long], :exception-types [], :flags #{:static :public}}
            #clojure.reflect.Field{:name SPLIT_SLOP, :type double, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:private :static :final}}
            #clojure.reflect.Field{:name INPUT_DIR, :type java.lang.String, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:static :public :final}}
            #clojure.reflect.Method{:name setInputPathFilter, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job java.lang.Class], :exception-types [], :flags #{:static :public}}
            #clojure.reflect.Method{:name setInputPaths, :return-type void, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.Job org.apache.hadoop.fs.Path<>], :exception-types [java.io.IOException], :flags #{:static :public :varargs}}
            #clojure.reflect.Method{:name getSplits, :return-type java.util.List, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext], :exception-types [java.io.IOException], :flags #{:public}}
            #clojure.reflect.Method{:name getInputPathFilter, :return-type org.apache.hadoop.fs.PathFilter, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext], :exception-types [], :flags #{:static :public}} #clojure.reflect.Field{:name LOG, :type org.apache.commons.logging.Log, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:private :static :final}} #clojure.reflect.Method{:name getMaxSplitSize, :return-type long, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext], :exception-types [], :flags #{:static :public}} #clojure.reflect.Method{:name isSplitable, :return-type boolean, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.mapreduce.JobContext org.apache.hadoop.fs.Path], :exception-types [], :flags #{:protected}} #clojure.reflect.Field{:name hiddenFileFilter, :type org.apache.hadoop.fs.PathFilter, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:private :static :final}} #clojure.reflect.Field{:name SPLIT_MINSIZE, :type java.lang.String, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:static :public :final}} #clojure.reflect.Method{:name makeSplit, :return-type org.apache.hadoop.mapreduce.lib.input.FileSplit, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :parameter-types [org.apache.hadoop.fs.Path long long java.lang.String<>], :exception-types [], :flags #{:protected}} #clojure.reflect.Field{:name SPLIT_MAXSIZE, :type java.lang.String, :declaring-class org.apache.hadoop.mapreduce.lib.input.FileInputFormat, :flags #{:static :public :final}}}}

  )



