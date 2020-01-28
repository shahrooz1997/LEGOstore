#!/bin/bash

var="./out/"
var="$var$2"
mkdir $var
var="$var"
echo $var

python3 placement.py -f $1 -o "$var/opt.json" -H min_cost
#python3 placement.py -f $1 -o "$var/opt-l.json" -H min_latency
echo "OPT Done"
python3 placement.py -f $1 -o "$var/abd.json" -H min_cost -b -t abd
#python3 placement.py -f $1 -o "$var/abd-l.json" -H min_latency -b -t abd
echo "ABD Done"
python3 placement.py -f $1 -o "$var/cas.json" -H min_cost -b -t cas
#python3 placement.py -f $1 -o "$var/cas-l.json" -H min_latency -b -t cas
echo "CAS Done"
python3 placement.py -f $1 -o "$var/rep.json" -H min_cost -b -t cas -k 1
#python3 placement.py -f $1 -o "$var/rep-l.json" -H min_latency -b -t cas -k 1
echo "REPL Done"
python3 placement.py -f $1 -o "$var/fixed-rep.json" -H min_cost -b -t cas -k 1 -m 7
#python3 placement.py -f $1 -o "$var/rep-l.json" -H min_latency -b -t cas -k 1 -m 7
echo "REPL Done"
