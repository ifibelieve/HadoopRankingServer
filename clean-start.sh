#!/bin/sh
ROOT=$(dirname $(pwd))
$ROOT/bin/hadoop fs -rm -r out*
javac -cp $($ROOT/bin/hadoop classpath) -d class/ src/*.java
jar -cvf Rank.jar -C class .

$ROOT/bin/hadoop fs -test -e input
if [ "$?" == "0" ]
then
	echo "exist"
else
	echo "not exist"
	$ROOT/bin/hadoop fs -mkdir input
	$ROOT/bin/hadoop fs -put sample input/
fi

$ROOT/bin/hadoop jar Rank.jar my.hadoop.test.Rank input out1 out2 10 
$ROOT/bin/hadoop fs -cat ./out2/*
