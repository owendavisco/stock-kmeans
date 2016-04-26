#!/bin/bash
USER="owenrd"
if [ -n "$1" ]
then
    USER=$1
fi

scp ../kmeans.py "$USER"@montpelier.cs.colostate.edu:~/Documents/
ssh "$USER"@montpelier.cs.colostate.edu "/usr/local/spark/bin/pyspark --master local[8] Documents/kmeans.py /s/bach/n/under/owenrd/Documents/stock"
