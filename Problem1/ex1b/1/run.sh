javac -classpath $(hadoop classpath) HadoopWordCount.java

jar -cvf WordCount.jar *.class

rm -r *.class

hadoop jar WordCount.jar HadoopWordCount ~/Wikipedia-En-41784-Articles/AA ./output_1b1

