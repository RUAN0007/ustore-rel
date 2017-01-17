CXX = g++
LD = g++

BUILD_DIR = bin

OBJ_DIR = obj
OBJ_SRC_DIR = $(OBJ_DIR)/src
OBJ_TEST_DIR = $(OBJ_DIR)/test

SRC_DIR = src
TEST_SRC_DIR = test

CFLAGS = -Wall -std=c++11 -Iinclude  -MMD
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
-include $(TEST_OBJS:%.o=%.d)

# all: dir $(EXEC)
all: $(EXEC) $(TEST_EXEC)

$(EXEC): $(OBJS) $(EXEC_SRC) | dir
	$(LD) $(CFLAGS) $^ $(LDFLAGS) -o $@

$(OBJS):$(OBJ_SRC_DIR)/%.o : $(SRC_DIR)/%.cpp 
	$(CXX) $(CFLAGS) -c $< -o $@

$(TEST_EXEC): $(OBJS) $(TEST_OBJS) $(TEST_EXEC_SRC) 
	$(LD) $(CFLAGS) $^ $(TEST_LDFLAGS) -o $@

$(TEST_OBJS):$(OBJ_TEST_DIR)/%.o : $(TEST_SRC_DIR)/%.cpp 
	$(CXX) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJ_SRC_DIR)/*
	rm -rf $(OBJ_TEST_DIR)/*
	rm -rf $(BUILD_DIR)/*

.PHONY: clean dir

.DEFAULT_GOAL = all