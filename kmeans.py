from __future__ import print_function
import sys
import os
from pyspark import SparkContext


def mapper(line):
    line_split = line.split(',')
    percent_change = -float(line_split[1])/float(line_split[4]) \
        if line_split[1] < line_split[4] \
        else float(line_split[4])/float(line_split[1])
    return line_split[0], percent_change

if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    map_results = sc.emptyRDD()
    for filename in os.listdir(sys.argv[1]):
        lines = sc.textFile(filename, 1)
        map_results.union(lines[1:].map(mapper))

    sc.stop()