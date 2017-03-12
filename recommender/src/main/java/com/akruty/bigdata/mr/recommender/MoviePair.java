package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vpati011 on 3/12/17.
 */
public class MoviePair implements WritableComparable<MoviePair> {

    private int movie1ID = 0;
    private int movie2ID = 0;

    public MoviePair() {
        movie1ID = movie2ID = 0;
    }

    public MoviePair(int movie1ID, int movie2ID) {
        this.movie1ID = movie1ID;
        this.movie2ID = movie2ID;
    }

    public int getMovie1ID() {
        return this.movie1ID;
    }

    public int getMovie2ID() {
        return this.movie2ID;
    }

    @Override
    public String toString() {
        return String.format("%d:%d", movie1ID, movie2ID);
    }

    @Override
    public int hashCode() {
        int result = this.movie1ID;
        result = 163*result + this.movie2ID;
        return result;
    }

    @Override
    public int compareTo(MoviePair mp) {
        int cmp = Integer.compare(this.movie1ID, mp.movie1ID);
        if (cmp != 0) {
            return cmp;
        }

        return Integer.compare(this.movie2ID, mp.movie2ID);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MoviePair == false) {
            return false;
        }

        MoviePair moviePair = (MoviePair)o;
        return moviePair.compareTo(this) == 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(movie1ID);
        dataOutput.writeInt(movie2ID);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.movie1ID = dataInput.readInt();
        this.movie2ID = dataInput.readInt();
    }

    public long Long() {
        long moviePair = this.movie1ID;
        moviePair = (moviePair << 32) | this.movie2ID;
        return moviePair;
    }

    public static MoviePair parseLong(long moviePair) {
        int movie1 = (int)(moviePair >> 32);
        int movie2 =  (int)(moviePair & 0xffffffff);
        return new MoviePair(movie1, movie2);
    }
}
