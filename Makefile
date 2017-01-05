CXX = g++
LD = g++

BUILD_DIR = bin

OBJ_DIR = obj
OBJ_SRC_DIR = $(OBJ_DIR)/src
OBJ_TEST_DIR = $(OBJ_DIR)/test

SRC_DIR = src
TEST_SRC_DIR = test

CFLAGS = -Wall -std=c++11 -Iinclude -MMD
LDFLAGS = 
TEST_LDFLAGS = $(LDFLAGS) -l gtest -pthread

SRCS=$(wildcard $(SRC_DIR)/*.cpp)
OBJS=$(addprefix $(OBJ_SRC_DIR)/, $(notdir $(SRCS:%.cpp=%.o)))
EXEC=$(BUILD_DIR)/main
EXEC_SRC=main.cpp
-include $(OBJS:%.o=%.d)

TEST_SRCS=$(wildcard $(TEST_SRC_DIR)/*.cpp)
TEST_OBJS=$(addprefix $(OBJ_TEST_DIR)/, $(notdir $(TEST_SRCS:%.cpp=%.o)))
TEST_EXEC=$(BUILD_DIR)/test
TEST_EXEC_SRC=test.cpp

# all: dir $(EXEC)
all: dir $(EXEC) $(TEST_EXEC)

dir: 
	mkdir -p $(BUILD_DIR)/
	mkdir -p $(OBJ_SRC_DIR)/
	mkdir -p $(OBJ_TEST_DIR)/

$(EXEC): $(OBJS) $(EXEC_SRC) | dir
	$(LD) $(CFLAGS) $^ $(LDFLAGS) -o $@

$(OBJS):$(OBJ_SRC_DIR)/%.o : $(SRC_DIR)/%.cpp | dir
	$(CXX) $(CFLAGS) -c $< -o $@

$(TEST_EXEC): $(OBJS) $(TEST_OBJS) $(TEST_EXEC_SRC) | dir
	$(LD) $(CFLAGS) $^ $(TEST_LDFLAGS) -o $@

$(TEST_OBJS):$(OBJ_TEST_DIR)/%.o : $(TEST_SRC_DIR)/%.cpp | dir
	$(CXX) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR)
	rm -rf $(OBJ_DIR)

.PHONY: clean dir