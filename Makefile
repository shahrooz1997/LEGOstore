CXX = g++
CXXFLAGS = -std=c++11 -Iinc -g `pkg-config --cflags protobuf`
LDFLAGS = -lm -lerasurecode -lrocksdb -Llib -lpthread -ldl `pkg-config --libs protobuf`


src = $(wildcard src/*.cpp)
obj = $(patsubst src/%.cpp, obj/%.o, $(src)) #obj_t = $(src:src=obj)

#obj2 = $(patsubst src/%.cc, obj/%.o, $(filter %.cc, $(src))) #obj_t = $(src:src=obj)
#bj = $(addsuffix .o, $(basename $(filter .cpp .cc, $(src))))

.PHONY: all
all: obj LEGOStore

obj: 
	mkdir obj

LEGOStore: $(obj) 
	$(CXX) -o $@ $^ $(LDFLAGS)

# Create object files
$(obj): obj/%.o: src/%.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

# For testing purposes
ABD: obj src/ABD.cpp
	$(CXX) -c $(CXXFLAGS) src/ABD.cpp -o obj/ABD.o

.PHONY: clean
clean:
	rm -rf obj LEGOStore
