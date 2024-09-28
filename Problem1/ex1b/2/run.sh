javac -classpath $(hadoop classpath) HadoopWordPairs.java

jar -cvf WordCount.jar *.class

rm -r *.class

hadoop jar WordCount.jar HadoopWordPairs ~/Wikipedia-En-41784-Articles/AA ./output_1b2

