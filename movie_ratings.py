#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.job import MRStep


class MostPopularMovie(MRJob):

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
        yield max(movie_watch_cnt_list)


if __name__ == "__main__":
    MostPopularMovie.run()
