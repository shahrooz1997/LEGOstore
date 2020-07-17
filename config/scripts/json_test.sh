#!/usr/bin/env bash

echo "the lenght of input json is $len"
extern=9.10.45.009

i=0
for server in $(gcloud compute instances list --format="value(name, zone, EXTERNAL_IP)")
do
	read names[$i] zones[$i] ips[$i] <<< "$server"
	(( i++))
done

echo ${names[*]}

exit 1

while [ $i -lt $len ]
do
	contents=$(echo ${contents} | jq --arg ip "$extern" --arg idx "$i" '.[$idx].servers."1".host = $ip')
	((i++))
done

echo $contents | jq '.' > $1
exit 0
