CXX = g++
CXXFLAGS =-Wall -std=c++11 -Iinc -g `pkg-config --cflags protobuf`
LDFLAGS = -lm -lerasurecode -lrocksdb -ldl -lpthread `pkg-config --libs protobuf`


src = $(wildcard src/*.cpp)
obj = $(patsubst src/%.cpp, obj/%.o, $(src)) #obj_t = $(src:src=obj)

#obj2 = $(patsubst src/%.cc, obj/%.o, $(filter %.cc, $(src))) #obj_t = $(src:src=obj)
#bj = $(addsuffix .o, $(basename $(filter .cpp .cc, $(src))))
#src1 = main.o

#src2 = Data_Server.o
obj2 = $(filter-out obj/Client_driver.o obj/Controller.o obj/Metadata_Server.o obj/gbuffer.pb.o,  $(obj))
obj1 = $(filter-out obj/Server_driver.o obj/Controller.o obj/Metadata_Server.o obj/gbuffer.pb.o, $(obj))
obj3 = $(filter-out obj/Client_driver.o obj/Server_driver.o obj/Metadata_Server.o obj/gbuffer.pb.o, $(obj))
obj4 = $(filter-out obj/Client_driver.o obj/Server_driver.o obj/Controller.o obj/gbuffer.pb.o, $(obj))

.PHONY: all
all: logs obj client server controller metadata_Server

.PHONY: debug
debug: CXXFLAGS += -g -O0 -D_GLIBCXX_DEBUG # debug flags
debug: all

LEGOStore: $(obj) 
	$(CXX) -o $@ $^ $(LDFLAGS)

logs:
	mkdir logs

metadata_Server: obj Metadata_Server
Metadata_Server: $(obj4) obj/gbuffer.pb.o
	$(CXX) -o $@ $^ $(LDFLAGS)

client : obj Client
Client: $(obj1) obj/gbuffer.pb.o
	$(CXX) -o $@ $^ $(LDFLAGS)

server: obj Server
Server: $(obj2) obj/gbuffer.pb.o
	$(CXX) -o $@ $^ $(LDFLAGS)

controller: obj Controller
Controller: $(obj3) obj/gbuffer.pb.o
	$(CXX) -o $@ $^ $(LDFLAGS)

obj:
	mkdir obj

obj/gbuffer.pb.o: src/gbuffer.pb.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

src/gbuffer.pb.cpp:
	protoc -I=inc --cpp_out=inc inc/gbuffer.proto
	mv inc/gbuffer.pb.cc src/gbuffer.pb.cpp

# Create object files
obj/%.o: src/%.cpp src/gbuffer.pb.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

# For testing purposes
ABD: obj src/ABD.cpp
	$(CXX) -c $(CXXFLAGS) src/ABD.cpp -o obj/ABD.o

.PHONY: clean cleandb cleanall
cleanall: clean cleandb

clean:
	rm -rf obj Client Server Controller LEGOStore Metadata_Server inc/gbuffer.pb.h src/gbuffer.pb.cpp

cleandb:
	rm -rf db*.temp
	rm -rf server*_output.txt
	rm -rf client*_output.txt
	rm -rf cont_output_*.txt
	rm -rf logfile_*.txt
	rm -rf logs/*
	rm -rf cont_logs/*
	rm -rf knossos
