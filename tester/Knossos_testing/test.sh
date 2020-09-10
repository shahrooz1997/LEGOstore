#!/bin/bash

# cd /home/shahrooz/Desktop/PSU/Research/LEGOstore/tester
./history_converter.py

flag=0

if [ ! -d "../../knossos" ] 
then
	cd ../../; git clone git@github.com:jepsen-io/knossos.git; cd ./tester/Knossos_testing
	flag=1
fi

mv output.txt ../../knossos/data/cas-register
cd ../../knossos/data/cas-register

pathk=`pwd`
# echo $pathk

lein run --model cas-register $pathk/output.txt
rm -f output.txt

if [ $flag -eq 1 ]
then
	cd ../../../
	rm -rf knossos
fi