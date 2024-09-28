import subprocess
import os
from datetime import datetime
import shutil

base_path = "/Users/zahra/Desktop/Uni Lu/Big Data/Wikipedia-En-41784-Articles"
wiki_sub_dirs = ['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK']
classpath = ":".join([
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/common/hadoop-common-2.7.7.jar",
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.7.jar",
    "/Users/zahra/.sdkman/candidates/hadoop/2.7.7/share/hadoop/common/lib/commons-logging-1.1.3.jar"
])

def compile_java(java_file):
    try:
        subprocess.check_call(['javac', '-classpath', classpath, java_file])
        print(f"{java_file} compiled successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Compilation failed: {e}")

def create_jar(jar_name, class_files):
    try:
        subprocess.check_call(['jar', 'cvf', jar_name] + class_files)
        print(f"JAR file {jar_name} created successfully!")
    except subprocess.CalledProcessError as e:
        print(f"JAR creation failed: {e}")

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

compile_java('HadoopWordCount.java')
create_jar('WordCount.jar', ['HadoopWordCount$Map.class', 
                              'HadoopWordCount$Reduce.class', 
                              'HadoopWordCount.class'])
subprocess.call('rm -r *.class', shell=True)

output = "runtimes.txt"

for i in range(len(wiki_sub_dirs)):

    input_paths = [os.path.join(base_path, subdir) for subdir in wiki_sub_dirs[:i+1]]
    output_dir = os.path.join(base_path, f"output_{wiki_sub_dirs[i]}")
    
    # if the output for that sub directory exists, it has to be deleted.
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    
    runtime = run_hadoop('WordCount.jar', 'HadoopWordCount', input_paths, output_dir)
    
    with open(output, 'a') as txtfile:
        txtfile.write(f"{' '.join(wiki_sub_dirs[:i+1])}, {runtime}\n")
