#!/bin/bash

rm -f latencies.txt

name1="latencies_from_server_"
name2=".txt"

for i in {1..9}
do
	cat $name1$i$name2 >> latencies.txt
done
