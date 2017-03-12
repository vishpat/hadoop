package com.akruty.bigdata.mr.recommender.job1;

import com.akruty.bigdata.mr.recommender.MovieRating;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by vpati011 on 3/12/17.
 */
public class UserMovieMapper extends Mapper<Object, Text, IntWritable, MovieRating> {

    @Override
    public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(),",");
        try {
            IntWritable userID = new IntWritable(Integer.parseInt(stringTokenizer.nextToken()));
            int movieID = Integer.parseInt(stringTokenizer.nextToken());
            float rating = Float.parseFloat(stringTokenizer.nextToken());
            MovieRating movieRating = new MovieRating(movieID, rating);
            ctx.write(userID, movieRating);
        } catch (NumberFormatException nex) {
            return;
        }
    }
}

