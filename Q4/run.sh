#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /Q4/output
$hadoop jar ./build/libs/Q4-1.0-SNAPSHOT.jar AvgAQI /input /Q4/output/
$hadoop fs -cat /Q4/output/part-r-00000 > out.txt