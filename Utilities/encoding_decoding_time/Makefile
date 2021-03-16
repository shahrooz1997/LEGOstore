TARGET = liberasure_measure
SRC_ADDR = ../../src
OBJS += Util.o Liberasure.o main.o

CXX = g++
# CXXFLAGS += -MMD -MP # dependency tracking flags
CXXFLAGS += -I./ -I../../inc
CXXFLAGS += -std=c++11 -DLIBRASURE_ONLY=1
LDFLAGS += -lm -lerasurecode -lrocksdb -ldl -lpthread `pkg-config --libs protobuf`

# all: CXXFLAGS += -g -O2 # release flags
# all: $(TARGET)

.PHONY: all
all: $(TARGET)

release: clean all

debug: CXXFLAGS += -g -O0 -D_GLIBC_DEBUG # debug flags
debug: clean $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# %.o: %.c
# 	$(CXX) $(CXXFLAGS) -c -o $@ $<

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

Util.o: $(SRC_ADDR)/Util.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

Liberasure.o: $(SRC_ADDR)/Liberasure.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	-@rm $(TARGET) $(OBJS) $(TARGET).d 2> /dev/null || true

run: all
	@clear
	@./$(TARGET)
