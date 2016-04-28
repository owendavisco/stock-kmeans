from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from datetime import datetime
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array


def mapper(line, filename):
    line_split = line.split(',')

    open = float(line_split[1])
    close = float(line_split[4])

    # if the first value is greater than the second it must be a negative percentage, otherwise calculate
    # percentage normally (open/close if open < close else -close/open)
    percent_change = 0.0

    if open > close:
        percent_change = (1 - (close / open)) * -1
    else:
        percent_change = 1 - (open / close)

    # send back the key value tuple with the key being the date and the value is the date and percentage change
    # ie. <"12/31/2010", [(12/31/2010).toMilliSeconds(), percentage_change]>
    #return line_split[0], [(datetime.strptime(line_split[0], "%Y-%m-%d").strftime('%s'), percent_change)]

    return filename, [percent_change]


def reducer(a, b):
    return a + b

if __name__ == "__main__":
    # Create the spark context
    sc = SparkContext(appName="PythonWordCount")

    # Create and empty RDD so that we can read all the files as one map and one reduce
    # RDDs are just abstractions of normal object that allow operations such as filter, map, and reduce
    # to be called on them. They can be combined using .union() which is performed later.
    map_results = sc.emptyRDD()

    # Read the directory of the stock data given by the user eg. kmeans.py "/Documents/stock/"
    directory = os.listdir(sys.argv[1])

    results = []

    # For every file in the directory, add it to the RDD so that we can perform operations on it
    for filename in directory:
        try:
            # lines is the file given the filename
            lines = sc.textFile("file:///" + os.path.join(sys.argv[1], filename), 1)

            # remove the head from the files because it is not actually data (we don't want the header)
            header = lines.first()
            lines = lines.filter(lambda line: line != header)

            # Add this file that has been properly mapped to the list of all mapped results
            result = lines.map(lambda line: mapper(line, filename.split(".")[0]))
            #map_results = map_results.union(result)
            results.append(result.cache())
        except Exception as e:
            print("Error with file " + filename)
            print(e.message)


    map_results = sc.union(results)

    map_results = map_results.reduceByKey(reducer).cache()

    """
    print("--------------------------------------------------------------------------------")
    for (x, y) in map_results.collect():
        if len(y) != 2517:
            print("Owen davis is thirsty", x, len(y))
    print("--------------------------------------------------------------------------------")
    """

    clusters = KMeans.train(map_results.values(), int(map_results.count()/10), maxIterations=300, runs=10, initializationMode="random")

    #map_results.saveAsTextFile("out")

    sc.stop()
