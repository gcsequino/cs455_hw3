#! /bin/bash

hadoop=$HADOOP_HOME/bin/hadoop

gradle build
$hadoop fs -rm -r /Q5/output
$hadoop jar ./build/libs/Q5-1.0-SNAPSHOT.jar Q5 /input /Q5/output/
$hadoop fs -cat /Q5/output/part-r-00000 > out.txt