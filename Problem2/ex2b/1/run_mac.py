import subprocess
import os
from datetime import datetime
import glob
import shutil


base_path = os.path.expanduser("/Users/zahra/Desktop/Uni Lu/Big Data/Wikipedia-En-41784-Articles")
wiki_sub_dirs = ['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK']

spark_home = os.environ.get('SPARK_HOME')
if not spark_home:
    raise EnvironmentError("SPARK_HOME environment variable is not set.")

def compile_scala(scala_file):
    # the classpath
    # $(echo $SPARK_HOME/jars/*.jar | tr ' ' ':')
    classpath = ':'.join([os.path.join(spark_home, 'jars', jar) for jar in os.listdir(os.path.join(spark_home, 'jars'))])
    try:
        subprocess.check_call(['scalac', '-classpath', classpath, scala_file])
        print(f"{scala_file} compiled successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Compilation failed: {e}")

def create_jar(jar_name):
    class_files = glob.glob('*.class')
    try:
        subprocess.check_call(['jar', 'cvf', jar_name] + class_files)
        print(f"JAR file {jar_name} created successfully!")
    except subprocess.CalledProcessError as e:
        print(f"JAR creation failed: {e}")

def run_spark(jar_file, main_class, input_paths, output_dir):
    try:
        input_paths = ','.join(input_paths) # for reading multiple inputs
        start_time = datetime.now()
        subprocess.check_call(['spark-submit', '--class', main_class, jar_file, input_paths, output_dir])
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        return duration
    except subprocess.CalledProcessError as e:
        print(f"Error running Spark job: {e}")
        return None

compile_scala('SparkWordCount.scala')
create_jar('SparkWordCount.jar')
subprocess.call('rm -r *.class', shell=True)

output_txt = "runtimes.txt"

for i in range(len(wiki_sub_dirs)):
    input_paths = [os.path.join(base_path, subdir) for subdir in wiki_sub_dirs[:i+1]]
    output_dir = os.path.join(base_path, f"output_{wiki_sub_dirs[i]}")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    runtime = run_spark('SparkWordCount.jar', 'SparkWordCount', input_paths, output_dir)
    
    with open(output_txt, 'a') as txtfile:
        txtfile.write(f"{' '.join(wiki_sub_dirs[:i+1])}, {runtime}\n")
