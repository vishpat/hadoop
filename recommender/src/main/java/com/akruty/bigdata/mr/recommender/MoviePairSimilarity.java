package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vpati011 on 3/13/17.
 */
public class MoviePairSimilarity implements Writable {

    private float similarity;

    private int cnt;

    public MoviePairSimilarity() {
        similarity = 0.0f;
        cnt = 0;
    }

    public MoviePairSimilarity(float similarity, int cnt) {
        this.similarity = similarity;
        this.cnt = cnt;
    }

    public float getSimilarity() {
        return similarity;
    }

    public int getCnt() {
        return cnt;
    }

    @Override
    public String toString() {
        return String.format("%f:%d", this.similarity, this.cnt);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.similarity);
        dataOutput.writeInt(this.cnt);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.similarity = dataInput.readFloat();
        this.cnt = dataInput.readInt();
    }
}
