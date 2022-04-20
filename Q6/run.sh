#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /cs455/hw3/q6/intermediate /cs455/hw3/q6/final
$hadoop jar ./build/libs/Q6-1.0-SNAPSHOT.jar Q6 /cs455/hw3/input /cs455/hw3/q6/intermediate /cs455/hw3/q6/final
$hadoop fs -cat /cs455/hw3/q6/final/part-r-00000 > out.txt