#!/bin/bash

make -j 9 >/dev/null 2>&1

if [ $? != 0 ]; then
   make
   exit 1
fi

make cleandb; ./Server >server_output.txt 2>&1 &

./Metadata_Server 127.0.0.1 30000 >metadata_output.txt 2>&1 &

sleep 2

./Controller

killall Server Metadata_Server