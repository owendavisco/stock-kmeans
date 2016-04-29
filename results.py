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

    mapred_results = sc.textFile(sys.argv[1])
    mapred_results = mapred_results.map(mapper).collect()

    model = KMeansModel.load(sc, "file:///" + sys.argv[2])

    # Create mapping of clusters for every stock
    clusters = {}
    for (stock1, value) in mapred_results:
        stock1 = stock1.encode("ascii", "ignore")
        for (stock2, value2) in mapred_results:
            stock2 = stock2.encode("ascii", "ignore")
            if stock2 != stock1:
                if model.predict(array(value)) == model.predict(array(value2)):
                    print("Stock " + stock1 + " and " + stock2 + " are in the same cluster")
                    clusters[stock1] = clusters[stock1] + [stock2] if stock1 in clusters else [stock2]

    with open('result.json', 'w') as fp:
        json.dump(clusters, fp)
