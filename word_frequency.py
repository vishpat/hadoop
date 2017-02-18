#!/usr/bin/env python

from mrjob.job import MRJob

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == "__main__":
    MRWordFreqCount.run()
