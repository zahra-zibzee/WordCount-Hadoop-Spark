# to compile
scalac -classpath $(echo $SPARK_HOME/jars/*.jar | tr ' ' ':') SparkWordCount.scala

# create jar file
jar -cvf ./SparkWordCount.jar *.class

rm -r *.class

# now run
spark-submit --class SparkWordCount ./SparkWordCount.jar ~/Wikipedia-En-41784-Articles/AA ./output_2b3
