CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -g -I./include
SRC_DIR = src
TEST_DIR = tests
BUILD_DIR = build

SRCS    = $(SRC_DIR)/main.c \
          $(SRC_DIR)/parser.c \
          $(SRC_DIR)/executor.c \
          $(SRC_DIR)/storage.c

TEST_SRCS = $(TEST_DIR)/test_parser.c \
            $(TEST_DIR)/test_executor.c \
            $(SRC_DIR)/parser.c \
            $(SRC_DIR)/executor.c \
            $(SRC_DIR)/storage.c

TARGET  = sqlparser
TEST_TARGET = test_runner

.PHONY: all clean test valgrind

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^

test: $(TEST_SRCS)
	$(CC) $(CFLAGS) -o $(TEST_TARGET) $^
	./$(TEST_TARGET)

valgrind: $(TARGET)
	valgrind --leak-check=full --error-exitcode=1 ./$(TARGET) $(SQL)

clean:
	rm -f $(TARGET) $(TEST_TARGET)
	rm -f data/*.csv

run:
	./$(TARGET) $(SQL)

json:
	./$(TARGET) $(SQL) --json