import subprocess
import os
from datetime import datetime
import shutil

# Define your base directory and subdirectories
base_directory = "/Users/zahra/Desktop/Uni Lu/Big Data/Wikipedia-En-41784-Articles"
subdirs = ['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK']
classpath = ":".join([
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/common/hadoop-common-2.7.7.jar",
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.7.jar",
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/common/lib/commons-logging-1.1.3.jar"
])

# Function to compile Java code
def compile_java(java_file):
    try:
        subprocess.check_call(['javac', '-classpath', classpath, java_file])
        print(f"Compiled {java_file} successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Compilation failed: {e}")

# Function to create a JAR file
def create_jar(jar_name, class_files):
    try:
        subprocess.check_call(['jar', 'cvf', jar_name] + class_files)
        print(f"Created JAR file {jar_name} successfully.")
    except subprocess.CalledProcessError as e:
        print(f"JAR creation failed: {e}")

# Function to run Hadoop job
def run_hadoop(hadoop_jar, job_class, input_paths, output_dir):
    try:
        start_time = datetime.now()
        cmd = ['hadoop', 'jar', hadoop_jar, job_class] + input_paths + [output_dir]
        subprocess.check_call(cmd)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        return duration
    except subprocess.CalledProcessError as e:
        print(f"Error running Hadoop job: {e}")
        return None

# Compile Java files and create JAR
compile_java('HadoopWordCount.java')
create_jar('WordCount.jar', ['HadoopWordCount$Map.class', 'HadoopWordCount$Reduce.class', 'HadoopWordCount.class'])

# Clean up .class files
subprocess.call('rm -r *.class', shell=True)

# Text output file
output_txt = "runtimes.txt"

# Write header to the text file
with open(output_txt, 'w') as txtfile:
    txtfile.write('Subdirectories, Runtime (Seconds)\n')

# Run the jobs and append results to text file
for i in range(len(subdirs)):
    input_paths = [os.path.join(base_directory, subdir) for subdir in subdirs[:i+1]]
    output_dir = os.path.join(base_directory, f"output_{subdirs[i]}")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    
    runtime = run_hadoop('WordCount.jar', 'HadoopWordCount', input_paths, output_dir)
    
    # Log the runtime to the text file
    with open(output_txt, 'a') as txtfile:
        txtfile.write(f"{' '.join(subdirs[:i+1])}, {runtime}\n")
