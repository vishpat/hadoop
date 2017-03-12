package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vpati011 on 3/12/17.
 */
public class MovieRating implements WritableComparable {
    private int movieID;
    private float movieRating;

    public MovieRating() {
        this.movieID = 0;
        this.movieRating = 0.0f;
    }

    public MovieRating(int movieID, float movieRating) {
        this.movieID = movieID;
        this.movieRating = movieRating;
    }

    public int getMovieID() {
        return movieID;
    }

    public float getMovieRating() {
        return movieRating;
    }

    @Override
    public boolean equals(Object o) {

        if ((o instanceof MovieRating) == false) {
            return false;
        }

        MovieRating mr = (MovieRating)o;
        if (mr.getMovieID() == this.movieID &&
                Float.compare(mr.getMovieRating(), this.movieRating) == 0) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(Object o) {
        if ((o instanceof MovieRating) == false) {
            return -1;
        }

        MovieRating movieRating = (MovieRating)o;
        if (movieRating.movieID != this.movieID) {
            return movieRating.movieID - this.movieID;
        }

        if (movieRating.movieRating != this.movieRating) {
            return movieRating.movieRating > this.movieRating ? 1 : -1;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.movieID);
        dataOutput.writeFloat(this.movieRating);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.movieID = dataInput.readInt();
        this.movieRating = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return String.format("%d:%f", movieID, movieRating);
    }
}

