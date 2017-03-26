package com.akruty.bigdata.mr.recommender.job2;

import com.akruty.bigdata.mr.recommender.MoviePair;
import com.akruty.bigdata.mr.recommender.MoviePairSimilarity;
import com.akruty.bigdata.mr.recommender.MovieRatingPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by vpati011 on 3/12/17.
 */
public class MoviePairReducer extends Reducer<MoviePair, MovieRatingPair, MoviePair, MoviePairSimilarity> {
    @Override
    public void reduce(MoviePair moviePair, Iterable<MovieRatingPair>movieRatingPairs, Context ctx)
            throws IOException, InterruptedException {
        int cnt = 0;
        Vector<Float> movie1Ratings = new Vector<>();
        Vector<Float> movie2Ratings = new Vector<>();

        for (MovieRatingPair movieRatingPair: movieRatingPairs) {
            movie1Ratings.add(movieRatingPair.getRating1());
            movie2Ratings.add(movieRatingPair.getRating2());
            cnt++;
        }

        float product = 1.0f;
        for (int i = 0; i < cnt; i++) {
            product = movie1Ratings.elementAt(i) * movie2Ratings.elementAt(i);
        }

        double movie1Size = Math.sqrt(movie1Ratings.stream().map(x -> x*x).reduce(1.0f, (x, y) -> x + y));
        double movie2Size = Math.sqrt(movie2Ratings.stream().map(x -> x*x).reduce(1.0f, (x, y) -> x + y));

        float similarity = (float)(product/(movie1Size*movie2Size));

        MoviePairSimilarity moviePairSimilarity = new MoviePairSimilarity(similarity, cnt);

        ctx.write(moviePair, moviePairSimilarity);
    }
}
