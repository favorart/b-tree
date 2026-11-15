# mydb: B-tree key-value storage
#
#   make            - build/libmydb.a and build/mydb_demo
#   make test       - build and run the unit and the integration tests
#   make SAN=1 test - the same with AddressSanitizer + UndefinedBehaviorSanitizer
#   make format     - clang-format the sources
#   make clean

CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=c99 -Wall -Wextra -D_XOPEN_SOURCE=700 -Iinclude -Isrc
LDFLAGS ?=

ifeq ($(SAN),1)
  override CFLAGS  += -O1 -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all
  override LDFLAGS += -fsanitize=address,undefined
endif

BUILD   := build
SOURCES := $(wildcard src/*.c)
OBJECTS := $(SOURCES:src/%.c=$(BUILD)/%.o)
HEADERS := $(wildcard include/*.h src/*.h)
LIB     := $(BUILD)/libmydb.a

.PHONY: all lib demo test format clean

all: lib demo

lib: $(LIB)
demo: $(BUILD)/mydb_demo

$(LIB): $(OBJECTS)
	$(AR) rcs $@ $^

$(BUILD)/%.o: src/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/%.o: examples/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/%.o: tests/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/mydb_demo: $(BUILD)/demo.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD)/test_%: $(BUILD)/test_%.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^

# the tests create their database files in build/
test: $(BUILD)/test_units $(BUILD)/test_mydb
	cd $(BUILD) && ./test_units && ./test_mydb

$(BUILD):
	mkdir -p $@

format:
	clang-format -i include/*.h src/*.c src/*.h examples/*.c tests/*.c

clean:
	rm -rf $(BUILD)
