#!/bin/bash

# cd /home/shahrooz/Desktop/PSU/Research/LEGOstore/tester
./history_converter.py

if [ ! -d "../../../knossos" ] 
then
	cd ../../../; git clone git@github.com:jepsen-io/knossos.git; cd LEGOstore/tester/Knossos_testing
fi

rm -rf ../../../knossos/converted_logs
cp -r converted_logs ../../../knossos/
cd ../../../knossos/converted_logs

pathk=`pwd`
# echo $pathk

files="$pathk/*"

coutner=0
number_of_files=0
for f in $files
do
	output=`../../LEGOstore/tester/Knossos_testing/lein run --model cas-register $f 2> /dev/null | grep 'false'`
	if [ $? == 0 ]
	then
		filename=`awk -F/ '{print $NF}' < $f`
		echo "not linearizable history found in key $filename"
		((coutner=counter-1))
	fi
	((counter=counter+1))
	((number_of_files=number_of_files+1))
done

if [ "$counter" -eq "$number_of_files" ]
then
	echo "The history is linearizable"
fi

cd ..; rm -rf converted_logs
