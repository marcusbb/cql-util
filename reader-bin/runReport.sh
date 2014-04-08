#!/bin/sh
#args
#1. column name - the name of the column
#2. print info on column count > threshold
#3. number of threads [optional] default 1

#first download the latest
#the current jar is built in a Jenkins Altus node
version=0.4.0
jarName=driver-util-$version-SNAPSHOT-all-dep.jar
localJar=driver-util-all.jar

if [ ! -f "$localJar" ]; then
	wget -O $localJar http://10.236.54.3:7001/job/CQL-util/ws/target/$jarName
fi
delim="		"
filename="report.csv"
threads=1
java -cp .:$localJar -Xmx2G reader.samples.CSVReportJob\$Main $filename $threads "$delim"
