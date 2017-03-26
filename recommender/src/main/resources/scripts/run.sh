#!/bin/bash

hadoop jar Recommender-1.0.jar -DSimilarityThreshold=0.4 -DCountThreshold=5 /movielens/ratings.csv /output /movilens/movies.csv 
