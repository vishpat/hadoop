package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Recommender {

    public static class RecommenderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    }

    public static void main(String[] args) {


    }
}
