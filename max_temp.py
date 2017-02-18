#!/usr/bin/env python

from mrjob.job import MRJob

class MaxTemp(MRJob):

    def mapper(self, _, line):
        location, time, type, temp, _, _, _, _ = line.split(',')
        yield location, temp 

    def combiner(self, location, temps):
        yield location, max(temps)

    def reducer(self, location, temps):
        yield location, max(temps)


if __name__ == "__main__":
    MaxTemp.run()
