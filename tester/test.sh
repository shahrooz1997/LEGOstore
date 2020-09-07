#!/bin/bash

# cd /home/shahrooz/Desktop/PSU/Research/LEGOstore/tester
./history_converter.py

if [ ! -d "../knossos" ] 
then
	cd ../; git clone git@github.com:jepsen-io/knossos.git; cd ./tester
fi

mv output.txt ../knossos/data/cas-register
cd ../knossos/data/cas-register

pathk=`pwd`
# echo $pathk

lein run --model cas-register $pathk/output.txt
rm -f output.txt