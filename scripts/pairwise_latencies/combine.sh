#!/bin/bash

rm -f latencies.txt

name1="latencies_from_server_"
name2="c.txt"

for i in {0..8}
do
	cat $name1$i$name2 >> latencies.txt
done
