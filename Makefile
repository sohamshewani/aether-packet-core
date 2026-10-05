CXX := g++
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -MMD -MP
LDFLAGS := 
TEST_LDFLAGS := -lstdc++

SRC_DIR := src
INC_DIR := include
BUILD_DIR := build
TEST_DIR := tests
BENCH_DIR := benchmarks

TARGET := $(BUILD_DIR)/aether-packet-core
TEST_TARGET := $(BUILD_DIR)/unit_tests
BENCH_TARGET := $(BUILD_DIR)/benchmark

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
DEPS := $(OBJS:.o=.d)

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/test_%.o, $(TEST_SRCS))

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS := $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/bench_%.o, $(BENCH_SRCS))

.PHONY: all build test clean benchmark directories

all: directories $(TARGET)

build: all

directories:
	@mkdir -p $(BUILD_DIR)

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -I$(INC_DIR) -c $< -o $@

$(BUILD_DIR)/test_%.o: $(TEST_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -I$(INC_DIR) -c $< -o $@

$(BUILD_DIR)/bench_%.o: $(BENCH_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -I$(INC_DIR) -c $< -o $@

test: directories $(TEST_OBJS)
	@if [ -n "$(TEST_OBJS)" ]; then \
		$(CXX) $(TEST_OBJS) -o $(TEST_TARGET) $(TEST_LDFLAGS) && $(TEST_TARGET); \
	else \
		echo "No test files found in $(TEST_DIR). Skipping tests."; \
	fi

benchmark: directories $(BENCH_OBJS)
	@if [ -n "$(BENCH_OBJS)" ]; then \
		$(CXX) $(BENCH_OBJS) -o $(BENCH_TARGET) $(LDFLAGS) && $(BENCH_TARGET); \
	else \
		echo "No benchmark files found in $(BENCH_DIR). Skipping benchmarks."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)

-include $(DEPS)