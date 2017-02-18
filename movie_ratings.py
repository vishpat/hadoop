#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.job import MRStep


class MostPopularMovie(MRJob):

    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--names')

    def steps(self):

        steps = [
            MRStep(mapper=self.mapper_movie,
                   reducer=self.reducer_movie_watch_cnt),
            MRStep(
                reducer=self.reducer_most_watched_movie)
        ]
        return steps

    def mapper_movie(self, _, line):
        user, movie, rating, ts = line.split(',')
        yield movie, 1

    def reducer_movie_watch_cnt(self, movie, watch_cnt):
        yield None, (sum(watch_cnt), movie)

    def reducer_most_watched_movie(self, m, movie_watch_cnt_list):
        view_cnt, movie_id = max(movie_watch_cnt_list)
        movie = None
        with open(self.options.names) as f:
            for line in f:
                movie_details = line
                if '|' in line:
                    parts = line.split('|')
                    movie_details = parts[0]
                parts = movie_details.split(',')

                if parts[0] == movie_id:
                    movie = parts[1]
                    break
        yield movie, view_cnt


if __name__ == "__main__":
    MostPopularMovie.run()
