import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HadoopWordCount extends Configured implements Tool {

    public static String wordRegex = "[a-zA-Z]+";

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\s+");

            for (String w : splitLine) {
                if (w.matches(wordRegex)) {
                    word.set(w);
                    context.write(word, one);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\s+");

            if (splitLine.length == 2) {
                String word = splitLine[0];
                int freq = Integer.parseInt(splitLine[1]);
                context.write(new IntWritable(freq), new Text(word));
            }
        }

    }


    public static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            for (Text value : values) {
                context.write(value, key);
            }
        }


    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(HadoopWordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        // job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            return 1; 
        }

        // Sort job
        Job sortJob = Job.getInstance(conf, "Sort Words");
        sortJob.setJarByClass(HadoopWordCount.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);

        sortJob.setMapperClass(SortMap.class);
        sortJob.setReducerClass(SortReduce.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(sortJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2]));

        sortJob.setMapOutputKeyClass(IntWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setNumReduceTasks(1);

        return sortJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
        System.exit(res);
    }
}
