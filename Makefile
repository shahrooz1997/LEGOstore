CXX = g++
CXXFLAGS = -std=c++11 -Iinc -g `pkg-config --cflags protobuf`
LDFLAGS = -lm -lerasurecode -lrocksdb -Llib -ldl -lpthread `pkg-config --libs protobuf`


src = $(wildcard src/*.cpp)
obj = $(patsubst src/%.cpp, obj/%.o, $(src)) #obj_t = $(src:src=obj)

#obj2 = $(patsubst src/%.cc, obj/%.o, $(filter %.cc, $(src))) #obj_t = $(src:src=obj)
#bj = $(addsuffix .o, $(basename $(filter .cpp .cc, $(src))))
#src1 = main.o
#src2 = Data_Server.o
obj2 = $(filter-out obj/Client.o,  $(obj))
obj1 = $(filter-out obj/Server.o, $(obj))

.PHONY: all
all: obj LEGOStore

LEGOStore: $(obj) 
	$(CXX) -o $@ $^ $(LDFLAGS)

client : obj Client
Client: $(obj1) 
	$(CXX) -o $@ $^ $(LDFLAGS)

server: obj Server
Server: $(obj2) 
	$(CXX) -o $@ $^ $(LDFLAGS)
obj: 
	mkdir obj

proto: serialize.proto
	mv inc/serialize.pb.cc src/serialize.pb.cpp

serialize.proto:
	protoc -I=inc --cpp_out=inc inc/serialize.proto
	

# Create object files
$(obj): obj/%.o: src/%.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

# For testing purposes
ABD: obj src/ABD.cpp
	$(CXX) -c $(CXXFLAGS) src/ABD.cpp -o obj/ABD.o

.PHONY: clean cleandb
clean:
	rm -rf obj Client Server LEGOStore

cleandb:
	rm -rf *.temp
