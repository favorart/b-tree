# mydb: B-tree key-value storage
#
#   make        - build/libmydb.a and build/mydb_demo
#   make format - clang-format the sources
#   make clean

CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=c99 -Wall -Wextra -Iinclude -Isrc
LDFLAGS ?=

BUILD   := build
SOURCES := $(wildcard src/*.c)
OBJECTS := $(SOURCES:src/%.c=$(BUILD)/%.o)
HEADERS := $(wildcard include/*.h src/*.h)
LIB     := $(BUILD)/libmydb.a

.PHONY: all lib demo format clean

all: lib demo

lib: $(LIB)
demo: $(BUILD)/mydb_demo

$(LIB): $(OBJECTS)
	$(AR) rcs $@ $^

$(BUILD)/%.o: src/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/%.o: examples/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/mydb_demo: $(BUILD)/demo.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD):
	mkdir -p $@

format:
	clang-format -i include/*.h src/*.c src/*.h examples/*.c

clean:
	rm -rf $(BUILD)
