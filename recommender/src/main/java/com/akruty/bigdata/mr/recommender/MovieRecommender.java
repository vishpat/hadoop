package com.akruty.bigdata.mr.recommender;

import com.akruty.bigdata.mr.recommender.job2.MoviePairMapper;
import com.akruty.bigdata.mr.recommender.job2.MoviePairReducer;
import com.akruty.bigdata.mr.recommender.job3.MoviePairFilterMapper;
import com.akruty.bigdata.mr.recommender.job3.SimilarMoviePairReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.akruty.bigdata.mr.recommender.job1.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by vpati011 on 3/11/17.
 */
public class MovieRecommender {

    private static final String JOB1_OUTPUT = "/mr/job1";

    private static final String JOB2_OUTPUT = "/mr/job2";

    private static final String JOB3_OUTPUT = "/mr/job3";

    private static final Logger LOGGER = Logger.getLogger(MovieRecommender.class);

    public static Map<Integer, String> getMovieMap(Configuration conf, String movieMapFile) throws IOException {

        Map<Integer, String> movieDB = new HashMap<>();
        Path moviesFilePath = new Path(movieMapFile);
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(moviesFilePath) == false) {
            throw new IllegalArgumentException(String.format("%s not found", moviesFilePath.toString()));
        }

        try(FSDataInputStream fsDataInputStream = hdfs.open(moviesFilePath)) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
                try {
                    movieDB.put(Integer.parseInt(stringTokenizer.nextToken()), stringTokenizer.nextToken());
                } catch (NumberFormatException nfex) {
                    continue;
                }
            }

            bufferedReader.close();
        }
        return movieDB;
    }

    public static void replaceMovieIDs(Configuration configuration,
                                       Map<Integer,String> movieMap, String inputPath,
                                       String outputPath) throws IOException {
        final Path movieSimilarityPath = new Path(String.format("%s/MovieSimilarities", outputPath));
        final FileSystem hdfs = FileSystem.get(configuration);

        if (hdfs.exists(movieSimilarityPath)) {
            hdfs.delete(movieSimilarityPath, true);
        }
        OutputStream os = hdfs.create(movieSimilarityPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));

        PathFilter pathFilter = path -> path.getName().startsWith("part");
        FileStatus[] fss = hdfs.listStatus(new Path(inputPath), pathFilter);
        for (FileStatus status: fss) {
            Path path = status.getPath();
            LOGGER.info(path.toString());
            try (FSDataInputStream fsDataInputStream = hdfs.open(path)) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
                String line;

                while ((line = bufferedReader.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
                    String movie1 = movieMap.get(Integer.parseInt(stringTokenizer.nextToken().trim()));
                    String movie2 = movieMap.get(Integer.parseInt(stringTokenizer.nextToken().trim()));
                    String similarity = stringTokenizer.nextToken();
                    String count = stringTokenizer.nextToken();
                    writer.write(String.format("%s, %s, %s, %s\n", movie1, movie2, similarity, count));
                }
                bufferedReader.close();
            }
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        Path outputPaths[] = {new Path(JOB1_OUTPUT),
                new Path(JOB2_OUTPUT), new Path(JOB3_OUTPUT), new Path(args[1])};
        for (Path outputPath: outputPaths) {
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
        }

        Job job1 = Job.getInstance(conf, "Movie Pair");
        job1.setJarByClass(MovieRecommender.class);

        job1.setMapperClass(UserMovieMapper.class);
        job1.setReducerClass(UserMovieReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(MovieRating.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(MovieRatingArrayWriteable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(JOB1_OUTPUT));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Movie Pair Rating");
        job2.setJarByClass(MovieRecommender.class);

        job2.setMapperClass(MoviePairMapper.class);
        job2.setReducerClass(MoviePairReducer.class);

        job2.setMapOutputKeyClass(MoviePair.class);
        job2.setMapOutputValueClass(MovieRatingPair.class);

        job2.setOutputKeyClass(MoviePair.class);
        job2.setOutputValueClass(MoviePairSimilarity.class);

        FileInputFormat.addInputPath(job2, new Path(JOB1_OUTPUT));
        FileOutputFormat.setOutputPath(job2, new Path(JOB2_OUTPUT));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "Movie Pair Filtering");
        job3.setJarByClass(MovieRecommender.class);

        job3.setMapperClass(MoviePairFilterMapper.class);
        job3.setReducerClass(SimilarMoviePairReducer.class);

        job3.setMapOutputKeyClass(MoviePair.class);
        job3.setMapOutputValueClass(MoviePairSimilarity.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(JOB2_OUTPUT));
        FileOutputFormat.setOutputPath(job3, new Path(JOB3_OUTPUT));
        job3.waitForCompletion(true);
        replaceMovieIDs(conf, getMovieMap(conf, args[2]), JOB3_OUTPUT, args[1]);
    }
}
