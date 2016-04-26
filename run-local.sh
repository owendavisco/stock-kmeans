#!/bin/bash
PROJECT_PATH="`pwd`"
SET="stock"

if [ -n "$1" ]
then
    SET=$1
fi

/usr/local/hadoop/bin/hadoop fs -rm -r /user/
/usr/local/spark/bin/spark-submit --master local[8] --driver-memory 3g --executor-memory 100G $PROJECT_PATH/kmeans.py $PROJECT_PATH/$SET
rm -r out
/usr/local/hadoop/bin/hadoop fs -get /user/$USER/out
