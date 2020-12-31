#!/bin/bash

rm -rf project
rm -rf project.tar.gz
mkdir project
cd project
mkdir logs
cd ../
cp -r ../inc ./project/
cp -r ../lib ./project/
cp -r ../src ./project/
cp -r ../config ./project/
rm -f ./project/src/gbuffer.pb.cpp ./project/inc/gbuffer.pb.h
cp ./setup_config.json ./project/config/auto_test/datacenters_access_info.json
cp ../Makefile ./project/
cp ./summarize.sh ./project

tar -czf project.tar.gz project/
