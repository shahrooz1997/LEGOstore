all: workers.cpp workers.h check.cpp
	g++ -o workers workers.cpp -lpthread -lrocksdb;
	g++ -o check check.cpp;
cleanall:
	rm workers check
check: check.cpp
	g++ -o check check.cpp
workers: workers.cpp workers.h
	g++ -o workers workers.cpp -lpthread -lrocksdb
clean: 
	rm workers
