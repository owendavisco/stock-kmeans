from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from datetime import datetime


def mapper(line):
    line_split = line.split(',')

    # if the first value is greater than the second it must be a negative percentage, otherwise calculate
    # percentage normally (open/close if open < close else -close/open)
    percent_change = -float(line_split[1])/float(line_split[4]) \
        if line_split[1] < line_split[4] \
        else float(line_split[4])/float(line_split[1])

    # send back the key value tuple with the key being the date and the value is the date and percentage change
    # ie. <"12/31/2010", [(12/31/2010).toMilliSeconds(), percentage_change]>
    return line_split[0], (datetime.strptime(line_split[0], "%Y-%m-%d").strftime('%s'), percent_change)

if __name__ == "__main__":
    # Create the spark context
    sc = SparkContext(appName="PythonWordCount")

    # Create and empty RDD so that we can read all the files as one map and one reduce
    # RDDs are just abstractions of normal object that allow operations such as filter, map, and reduce
    # to be called on them. They can be combined using .union() which is performed later.
    map_results = sc.emptyRDD()

    # Read the directory of the stock data given by the user eg. kmeans.py "/Documents/stock/"
    directory = os.listdir(sys.argv[1])

    # For every file in the directory, add it to the RDD so that we can perform operations on it
    for filename in directory:

        # lines is the file given the filename
        lines = sc.textFile("file:///" + os.path.join(sys.argv[1], filename), 1)

        # remove the head from the files because it is not actually data (we don't want the header)
        header = lines.first()
        lines = lines.filter(lambda line: line != header)

        try:
            # Add this file that has been properly mapped to the list of all mapped results
            result = lines.map(mapper)
            print(result)
            map_results.union(result)
        except Exception as e:
            print("Error with file " + filename)
            print(e.message)

    output = map_results.collect()
    for (word, count) in output:
        print(word + " " + count)

    print("---------------------------------------------")
    print("---------------------------------------------")
    print(output)
    print("---------------------------------------------")
    print("---------------------------------------------")

    sc.stop()
