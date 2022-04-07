#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /Q3/output
$hadoop jar ./build/libs/Q3-1.0-SNAPSHOT.jar AvgAQI /input /Q3/output/