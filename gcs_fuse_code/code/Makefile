.ONESHELL:
SHELL := /bin/bash

all: workers.cpp workers.h
	g++ -o workers workers.cpp -lpthread -lrocksdb;
clean: 
	rm workers;

# Below are specific to Rajveer's system
# You can configure it by cloning rocksdb and "make"ing it in debug mode and adding the path as below
DEBUG_HEADER_LOCATION = /home/rajveer/devspace/rocksdb/include
DEBUG_LIBRARY_LOCATION = /home/rajveer/devspace/rocksdb



debug: workers.cpp workers.h
	@if [ -d "$(DEBUG_HEADER_LOCATION)" ] && [ -d "$(DEBUG_LIBRARY_LOCATION)" ]
	then
		g++ -g -I $(DEBUG_HEADER_LOCATION) -L $(DEBUG_LIBRARY_LOCATION) -o workers workers.cpp -lrocksdb_debug -lpthread -ldl -lz;
	else
		echo "debug libraries absent";
	fi
