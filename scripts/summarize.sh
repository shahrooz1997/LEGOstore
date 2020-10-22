#!/bin/bash

run_type=$1

gets=$( cat logs/* | grep get)
puts=$( cat logs/* | grep put)
count=0
total=0

# echo $puts
Field_Separator=$IFS
IFS=$'\n'
for line in $puts
do
	# echo "SSSS$line"
	end_time=$( awk '{ print $6; }' <<< $line )
	start_time=$( awk '{ print $5; }' <<< $line )
	start_time=$(echo $start_time | sed 's/,//')
	temp=$((end_time - start_time))
	total=$(echo $total+$temp | bc );
	((count++))
done
IFS=$Field_Separator
res1=`echo "scale=2; $total / $count" | bc`
echo "put: total = $total, count = $count"

count=0
total=0
Field_Separator=$IFS
IFS=$'\n'
for line in $gets
do
	# echo "SSSS$line"
	end_time=$( awk '{ print $6; }' <<< $line )
	start_time=$( awk '{ print $5; }' <<< $line )
	start_time=$(echo $start_time | sed 's/,//')
	temp=$((end_time - start_time))
	total=$(echo $total+$temp | bc );
	((count++))
done
IFS=$Field_Separator
res2=`echo "scale=2; $total / $count" | bc`
echo "get: total = $total, count = $count"

echo "for $run_type, we have:"
echo "put latency is $res1"
echo "get latency is $res2"
echo