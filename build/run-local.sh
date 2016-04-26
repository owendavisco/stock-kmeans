#!/bin/bash
/usr/local/spark/bin/pyspark --master local[8] Documents/kmeans.py /s/bach/n/under/owenrd/Documents/stock > logs.txts
