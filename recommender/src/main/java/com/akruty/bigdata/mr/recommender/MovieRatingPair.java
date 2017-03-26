package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vpati011 on 3/12/17.
 */
public class MovieRatingPair implements WritableComparable<MovieRatingPair> {

    private float rating1;
    private float rating2;

    public MovieRatingPair() {
        rating1 = rating2 = 0.0f;
    }

    public MovieRatingPair(float rating1, float rating2) {
        this.rating1 = rating1;
        this.rating2 = rating2;
    }

    public float getRating1() {
        return rating1;
    }

    public float getRating2() {
        return rating2;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MovieRatingPair == false) {
            return false;
        }

        MovieRatingPair mrp = (MovieRatingPair)o;
        if (Float.compare(this.rating1, mrp.rating1) == 0 &&
                Float.compare(this.rating2, mrp.rating2) == 0) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(MovieRatingPair movieRatingPair) {
        if (this.equals(movieRatingPair)) {
            return 0;
        }

        return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(rating1);
        dataOutput.writeFloat(rating2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rating1 = dataInput.readFloat();
        rating2 = dataInput.readFloat();
    }
}
