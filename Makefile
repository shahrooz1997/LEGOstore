CXX = g++
CXXFLAGS = -std=c++11 -Iinc -g `pkg-config --cflags protobuf`
LDFLAGS = -lm -lerasurecode -Llib -lrocksdb -ldl -lpthread `pkg-config --libs protobuf`


src = $(wildcard src/*.cpp)
obj = $(patsubst src/%.cpp, obj/%.o, $(src)) #obj_t = $(src:src=obj)

#obj2 = $(patsubst src/%.cc, obj/%.o, $(filter %.cc, $(src))) #obj_t = $(src:src=obj)
#bj = $(addsuffix .o, $(basename $(filter .cpp .cc, $(src))))
#src1 = main.o
#src2 = Data_Server.o
obj2 = $(filter-out obj/main.o,  $(obj))
obj1 = $(filter-out obj/Data_Server.o, $(obj))

.PHONY: all
all: obj server client

LEGOStore: $(obj) 
	$(CXX) -o $@ $^ $(LDFLAGS)

client : obj CASClient
CASClient: $(obj1) 
	$(CXX) -o $@ $^ $(LDFLAGS)

server: obj CASServer
CASServer: $(obj2) 
	$(CXX) -o $@ $^ $(LDFLAGS)
obj: 
	mkdir obj


# Create object files
$(obj): obj/%.o: src/%.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

# For testing purposes
ABD: obj src/ABD.cpp
	$(CXX) -c $(CXXFLAGS) src/ABD.cpp -o obj/ABD.o

.PHONY: clean cleandb cleanall
cleanall: clean cleandb

clean:
	rm -rf obj CASClient CASServer LEGOStore

cleandb:
	rm -rf db*.temp

