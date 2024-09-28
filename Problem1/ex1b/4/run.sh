javac -classpath $(hadoop classpath) HadoopWordPairs.java

jar -cvf WordCount.jar *.class

rm -r *.class

hadoop jar WordCount.jar HadoopWordPairs ~/Wikipedia-En-41784-Articles/AA ./output_1b4_between ./output_1b4

tail -n 100 ./output_1b4/part-r-00000

