#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /Q1/output
$hadoop jar ./build/libs/Q1-1.0-SNAPSHOT.jar Q1 /input /Q1/output/
$hadoop fs -cat /Q1/output/part-r-00000 > out.txt