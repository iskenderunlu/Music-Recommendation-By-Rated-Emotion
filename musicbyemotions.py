#!/usr/bin/env python

import itertools
import sys
from operator import add
from math import sqrt
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

#def computeRmse(model, data, n):
#    """
#    Compute RMSE (Root Mean Squared Error).
#    """
#    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
#    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
#      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
#      .values()
#    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

def ratingsUploader(ratingsFile):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print "File %s does not exist." % ratingsFile
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [ratingParser(line)[1] for line in f])
    f.close()
    if not ratings:
        print "No ratings provided."
        sys.exit(1)
    else:
        return ratings

def ratingParser(line):
    """
    Parses a rating record in Mucicers format userId;;musicId;;rating;;year .
    """
    fields = line.strip().split(";;")
    return long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

def musicParser(line):
    """
    Parses a music record in Musicers format musicId;;musicTitle .
    """
    fields = line.strip().split(";;")
    return int(fields[0]), fields[1]



if __name__ == "__main__":
    if (len(sys.argv) != 3):
    	print "Usage: /path/to/spark/bin/spark-submit --driver-memory 2g " + "musicbyemotion.py MusicsDataDir personalRatesFileDir"
        sys.exit(1)

    # set up environment
    conf = SparkConf() \
      .setAppName("Musicers") \
      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # load personal ratings
    myRatings = ratingsUploader(sys.argv[2])
    myRatingsRDD = sc.parallelize(myRatings, 1)
    
    # load ratings and music titles

    MusicsHomeDir = sys.argv[1]

    # ratings is an RDD of (last digit of timestamp, (userId, musicId, rating))
    ratings = sc.textFile(join(MusicsHomeDir, "ratings.dat")).map(ratingParser)

    # musics is an RDD of (musicId, musicTitle)
    musics = dict(sc.textFile(join(MusicsHomeDir, "musics.dat")).map(musicParser).collect())

  
    # clean up
    sc.stop()
