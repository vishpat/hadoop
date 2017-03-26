package com.akruty.bigdata.mr.recommender.job2;

import com.akruty.bigdata.mr.recommender.MoviePair;
import com.akruty.bigdata.mr.recommender.MovieRatingPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by vpati011 on 3/12/17.
 */
public class MoviePairMapper extends Mapper<Object, Text, MoviePair, MovieRatingPair> {

    private final static Logger logger = Logger.getLogger(MoviePairMapper.class);

    class MovieRating {
        int movieID;
        float rating;

        MovieRating(int movieID, float rating) {
            this.movieID = movieID;
            this.rating = rating;
        }

        public int getMovieID() {
            return this.movieID;
        }

        public float getMovieRating() {
            return rating;
        }
    }

    @Override
    public void map(Object obj, Text movieRatingsText, Context ctx) throws IOException, InterruptedException {
            List<MovieRating> movieRatingList = new ArrayList<>();
            StringTokenizer stringTokenizer = new StringTokenizer(movieRatingsText.toString(), ",");
            while (stringTokenizer.hasMoreTokens()) {
                String movieRating = stringTokenizer.nextToken();
                StringTokenizer stringTokenizer1 = new StringTokenizer(movieRating, ":");
                MovieRating mr = new MovieRating(Integer.parseInt(stringTokenizer1.nextToken()),
                                        Float.parseFloat(stringTokenizer1.nextToken()));
                movieRatingList.add(mr);
            }

            movieRatingList.forEach((MovieRating mr1) -> {
                movieRatingList.stream().forEach(mr2 -> {
                    if (mr1.getMovieID() != mr2.getMovieID()) {
                        MoviePair moviePair = new MoviePair(mr1.getMovieID(), mr2.getMovieID());
                        MovieRatingPair movieRatingPair = new MovieRatingPair(mr1.getMovieRating(), mr2.getMovieRating());
                        try {
                            ctx.write(moviePair, movieRatingPair);
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage());
                        }

                        moviePair = new MoviePair(mr2.getMovieID(), mr1.getMovieID());
                        movieRatingPair = new MovieRatingPair(mr2.getMovieRating(), mr1.getMovieRating());
                        try {
                            ctx.write(moviePair, movieRatingPair);
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage());
                        }
                    }
                });
            });
    }
}
