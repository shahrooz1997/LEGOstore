#!/bin/bash

var="./tests/traces/trace_"
var="$var$2"
mkdir -p $var

python3 trace_gen.py -f $1 -i $2 
