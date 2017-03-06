package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Recommender {

    public static class RecommenderMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                ctx.write(word, one);
            }
        }
    }

    public static class RecommenderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }

            ctx.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        FileSystem hdfs = FileSystem.get(conf);
        Path outputDir = new Path(args[1]);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }

        Job job = Job.getInstance(conf, "Word count");
        job.setJarByClass(Recommender.class);
        job.setMapperClass(RecommenderMapper.class);
        job.setCombinerClass(RecommenderReducer.class);
        job.setReducerClass(RecommenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
