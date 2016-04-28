from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel

def mapper(line):
    # Format the line
    line = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    elements = line.split(",")
    stock_name = elements.pop(0)
    percent_changes = map(lambda x: float(x.trim()), elements)

    return stock_name, percent_changes

if __name__ == "__main__":
    sc = SparkContext(appName="ComputeResults")

    mapred_results = sc.textFile(sys.argv[1])
    mapred_results = mapred_results.map(mapper).collect()

    model = KMeansModel.load(sc, sys.argv[2])

    # Create mapping of clusters for every stock
    clusters = {}
    for (key, value) in mapred_results:
        for (key2, value2) in mapred_results:
            clusters.update()