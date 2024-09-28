# cd src/main/scala
# scalac -classpath $(echo $SPARK_HOME/jars/*.jar | tr ' ' ':') PartA.scala
# cd ..
# cd ..
# cd ..
# sbt package
# JAR_NAME=$(find . -name "*.jar" -print -quit)
# spark-submit --class PARTA "$JAR_NAME"

# Here's a complete script that compiles the Scala code, packages it, and then runs the spark-submit:
# This script will:
# Compile all Scala files found in src/main/scala/.
# Run sbt package to package the application into a JAR file.
# Find the resulting JAR file in the target/scala-2.12/ directory.
# Run spark-submit with the found JAR file and the specified main class.


#!/bin/bash

# Check if a class name is provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {ClassToRun}"
  exit 1
fi

# Class to run
CLASS_TO_RUN=$1

# Navigate to the root directory of the project
cd "$(dirname "$0")"

# Delete the output directory if it exists
OUTPUT_DIR="${CLASS_TO_RUN}-output"
if [ -d "$OUTPUT_DIR" ]; then
  echo "Deleting existing output directory $OUTPUT_DIR..."
  rm -r "$OUTPUT_DIR"
fi

# Run sbt package to create a JAR file
echo "Packaging the project with sbt..."
sbt package

# Assuming the JAR file is named according to the sbt build definition
# Find the jar file just created
JAR_NAME=$(find target/scala-2.12 -name "*.jar" -print -quit)

if [ ! -f "$JAR_NAME" ]; then
  echo "JAR file not found. Exiting."
  exit 1
fi

# Submit the Spark job
echo "Running spark-submit..."
spark-submit --class "$CLASS_TO_RUN" "$JAR_NAME"




# to run:
# chmod +x run_spark.sh
# ./run_spark.sh PARTA