package com.akruty.bigdata.mr.recommender;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by vpati011 on 3/12/17.
 */

public class MovieRatingArrayWriteable extends ArrayWritable {

      public MovieRatingArrayWriteable(Class<? extends Writable> valueClass, Writable[] values) {
          super(valueClass, values);
      }

      @Override
      public String toString() {
          StringBuffer sb = new StringBuffer();
          for (Writable wr: get()) {
              sb.append(String.format("%s,", wr));
          }
          return sb.toString();
      }
}
