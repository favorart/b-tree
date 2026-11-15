# mydb: B-tree key-value storage
#
#   make            - build/libmydb.a and build/mydb_demo
#   make test       - build and run the tests (cmocka)
#   make SAN=1 test - the same with AddressSanitizer + UndefinedBehaviorSanitizer
#   make bench      - throughput benchmark (BENCH_N keys, 200000 by default)
#   make format     - clang-format the sources
#   make clean

CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=c99 -Wall -Wextra -Wpedantic -D_XOPEN_SOURCE=700 -D_FILE_OFFSET_BITS=64 -Iinclude -Isrc
LDFLAGS ?=

# the version comes from the git tag: 0.0.5, or 0.0.5-3-gabc1234 three commits after it
VERSION ?= $(shell git describe --tags --match 'v[0-9]*' --always --dirty 2>/dev/null | sed 's/^v//')
override CFLAGS += -DMYDB_LIB_VERSION=\"$(or $(VERSION),unknown)\"

ifeq ($(SAN),1)
  override CFLAGS  += -O1 -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all
  override LDFLAGS += -fsanitize=address,undefined
endif

# the tests use cmocka from the system (apt: libcmocka-dev, brew: cmocka)
CMOCKA_CFLAGS ?= $(shell pkg-config --cflags cmocka 2>/dev/null)
CMOCKA_LIBS   ?= $(shell pkg-config --libs cmocka 2>/dev/null || echo -lcmocka)

BUILD   := build
SOURCES := $(wildcard src/*.c)
OBJECTS := $(SOURCES:src/%.c=$(BUILD)/%.o)
HEADERS := $(wildcard include/*.h src/*.h)
LIB     := $(BUILD)/libmydb.a

.PHONY: all lib demo test bench format clean

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
	$(CC) $(CFLAGS) $(CMOCKA_CFLAGS) -c $< -o $@

$(BUILD)/%.o: bench/%.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/mydb_demo: $(BUILD)/demo.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD)/test_%: $(BUILD)/test_%.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^ $(CMOCKA_LIBS)

# the tests create their database files in build/
test: $(BUILD)/test_units $(BUILD)/test_mydb $(BUILD)/test_worst
	cd $(BUILD) && ./test_units && ./test_mydb && ./test_worst

$(BUILD)/bench_mydb: $(BUILD)/bench_mydb.o $(LIB)
	$(CC) $(LDFLAGS) -o $@ $^

BENCH_N ?= 200000
bench: $(BUILD)/bench_mydb
	cd $(BUILD) && ./bench_mydb $(BENCH_N)

$(BUILD):
	mkdir -p $@

format:
	clang-format -i include/*.h src/*.c src/*.h examples/*.c tests/*.c bench/*.c

clean:
	rm -rf $(BUILD)
