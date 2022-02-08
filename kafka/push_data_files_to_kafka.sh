#!/bin/bash

export PATH=$PATH:/home/avs/Documents/kafka/bin
FILES=$1/*.csv

for f in $FILES
do 
	echo "pushing $f file"
	cat $f| kafka-console-producer.sh --bootstrap-server $2 --topic $3
	sleep 60
done
