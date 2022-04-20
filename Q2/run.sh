#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /Q2/output
$hadoop jar ./build/libs/Q2-1.0-SNAPSHOT.jar Q2 /input /Q2/output/
$hadoop fs -cat /Q2/output/part-r-00000 > out.txt