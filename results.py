from __future__ import print_function
import sys
import json
from numpy import array
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel


def mapper(line):
    # Format the line
    line = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    elements = line.split(",")
    stock_name = elements.pop(0)
    percent_changes = map(lambda x: float(x), elements)

    return stock_name, percent_changes


if __name__ == "__main__":
    sc = SparkContext(appName="ComputeResults")

    model = KMeansModel.load(sc, sys.argv[2])

    mapred_results = sc.textFile(sys.argv[1])
    clusters = mapred_results.map(mapper)\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda stock: (model.predict(array(stock[1])), [stock[0]]))\
        .reduceByKey(lambda a, b: a + b)\
        .collectAsMap()

    with open('result.json', 'w') as fp:
        json.dump(clusters, fp)
