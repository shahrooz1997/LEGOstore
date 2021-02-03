#!/bin/bash

rm -rf project
rm -rf project.tar.gz
mkdir project
cd project
mkdir logs
mkdir lib
cd ../
cp -r ../inc ./project/
cp -r ../src ./project/
cp -r ../config ./project/
rm -f ./project/src/gbuffer.pb.cpp ./project/inc/gbuffer.pb.h
cp ./setup_config.json ./project/config/auto_test/datacenters_access_info.json
cp ../Makefile ./project/
cp ./summarize.sh ./project
mkdir -p /tmp/LEGOSTORE_AUTORUN
cp -r ../config /tmp/LEGOSTORE_AUTORUN

tar -czf project2.tar.gz project/
