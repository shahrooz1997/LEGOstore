CXX = g++
CXXFLAGS = -std=c++11 -Iinc
LDFLAGS = -lm -lpthread

src = $(wildcard src/*.cpp)
obj= $(patsubst src/%.cpp, obj/%.o, $(src)) #obj_t = $(src:src=obj)
#obj = $(obj_t:.cpp=.o)

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
