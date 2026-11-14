# mydb: B-tree key-value storage
#
#   make            - static library libmydb.a and the demo
#   make test       - build and run the tests
#   make SAN=1 test - the same with AddressSanitizer + UndefinedBehaviorSanitizer
#   make format     - clang-format the sources
#   make clean

CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=c99 -Wall -Wextra -D_XOPEN_SOURCE=700 -I.
LDFLAGS ?=

ifeq ($(SAN),1)
  override CFLAGS  += -O1 -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all
  override LDFLAGS += -fsanitize=address,undefined
endif

LIB_SOURCES := mydb.c mydb_block.c mydb_block_delete.c mydb_block_low.c mydb_cache.c \
               mydb_error.c mydb_log.c mydb_methods.c mydb_techb.c
LIB_OBJECTS := $(LIB_SOURCES:.c=.o)

.PHONY: all lib demo test format clean

all: lib demo

lib: libmydb.a
demo: mydb_demo

libmydb.a: $(LIB_OBJECTS)
	$(AR) rcs $@ $^

mydb_demo: mydb_main.o libmydb.a
	$(CC) $(LDFLAGS) -o $@ $^

tests/test_mydb: tests/test_mydb.o libmydb.a
	$(CC) $(LDFLAGS) -o $@ $^

test: tests/test_mydb
	cd tests && ./test_mydb

%.o: %.c $(wildcard *.h)
	$(CC) $(CFLAGS) -c $< -o $@

format:
	clang-format -i *.c *.h tests/*.c

clean:
	rm -f *.o tests/*.o libmydb.a mydb_demo tests/test_mydb
	rm -f *.db *.db.log tests/*.db tests/*.db.log
