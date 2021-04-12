#!/bin/bash

rm -rf optimizer
rm -rf optimizer.tar.gz

cp -r ../optimizer ./
rm -f ./optimizer/Experiments_Lat_sense/workloads/f=*/res_*
rm -f ./optimizer/Experiments_Lat_sense/workloads/*_output*.txt
rm -rf ./optimizer/Experiments_Lat_sense/RESULTS
rm -rf ./optimizer/Experiments_Lat_sense/workloads?*
rm -rf ./optimizer/Experiments

# mkdir optimizer
# cd project
# mkdir logs
# mkdir lib
# cd ../
# cp -r ../inc ./project/
# cp -r ../src ./project/
# cp -r ../config ./project/
# cp -r ../Utilities ./project/
# cd ./project/Utilities/encoding_decoding_time; rm -f *.o liberasure_measure; cd ../../../
# rm -f ./project/src/gbuffer.pb.cpp ./project/inc/gbuffer.pb.h
# cp ./setup_config.json ./project/config/auto_test/datacenters_access_info.json
# cp ../Makefile ./project/
# cp ./summarize.py ./project
# mkdir -p /tmp/LEGOSTORE_AUTORUN
# rm -rf /tmp/LEGOSTORE_AUTORUN/config
# cp -r ../config /tmp/LEGOSTORE_AUTORUN

tar -czf optimizer.tar.gz optimizer/
