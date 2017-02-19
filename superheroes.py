#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.job import MRStep


class Superheroes(MRJob):

    def configure_options(self):
        super(Superheroes, self).configure_options()
        self.add_file_option('--names')

    def steps(self):

        steps = [
            MRStep(mapper=self.mapper_friends,
                   reducer=self.reducer_friend_cnt),
            MRStep(
                reducer=self.reducer_most_popular_superhero)
        ]
        return steps

    def mapper_friends(self, _, line):
        parts = line.split()
        yield parts[0], len(parts[1:])

    def reducer_friend_cnt(self, super_hero, friend_cnt):
        yield None, (sum(friend_cnt), super_hero)

    def reducer_most_popular_superhero(self, key, values):
        friend_cnt, superhero_id = max(values)
        superhero = None
        with open(self.options.names) as f:
            for line in f:
                parts = line.split()
                id = parts[0]
                name = ' '.join(parts[1:])
                if id == superhero_id:
                    superhero = name
                    break
        yield superhero, friend_cnt


if __name__ == "__main__":
    Superheroes.run()
