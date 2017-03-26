package com.akruty.bigdata.mr.recommender.job1;

import com.akruty.bigdata.mr.recommender.MovieRating;
import com.akruty.bigdata.mr.recommender.MovieRatingArrayWriteable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by vpati011 on 3/12/17.
 */

public class UserMovieReducer extends Reducer<IntWritable, MovieRating, IntWritable, MovieRatingArrayWriteable> {

    @Override
    public void reduce(IntWritable userID, Iterable<MovieRating>movieRatings, Context ctx)
            throws IOException, InterruptedException{
        ArrayList<MovieRating> movieRatingsList = new ArrayList<>();
        movieRatings.forEach(mr -> movieRatingsList.add(new MovieRating(mr.getMovieID(), mr.getMovieRating())));
        MovieRatingArrayWriteable movieRatingArrayWritable = new MovieRatingArrayWriteable(MovieRating.class,
                movieRatingsList.stream().toArray(WritableComparable[]::new));
        ctx.write(null, movieRatingArrayWritable);
    }
}
