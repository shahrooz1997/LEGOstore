.ONESHELL:
SHELL := /bin/bash

all: workers.cpp workers.h check.cpp
	g++ -o workers workers.cpp -lpthread -lrocksdb;
	g++ -o check check.cpp;
cleanall:
	rm workers check;
check: check.cpp
	g++ -o check check.cpp;
workers: workers.cpp workers.h
	g++ -o workers workers.cpp -lpthread -lrocksdb;
clean: 
	rm workers;

DEBUG_HEADER_LOCATION = /home/rajveer/devspace/rocksdb/include
DEBUG_LIBRARY_LOCATION = /home/rajveer/devspace/rocksdb



debug: workers.cpp workers.h
	@if [ -d "$(DEBUG_HEADER_LOCATION)" ] && [ -d "$(DEBUG_LIBRARY_LOCATION)" ]
	then
		g++ -g -I $(DEBUG_HEADER_LOCATION) -L $(DEBUG_LIBRARY_LOCATION) -o workers workers.cpp -lrocksdb_debug -lpthread -ldl -lz;
	else
		echo "debug libraries absent";
	fi
