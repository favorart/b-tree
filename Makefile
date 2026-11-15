# mydb: B-tree key-value storage
#
#   make        - libmydb.a and the demo mydb_demo
#   make clean

CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=c99 -Wall -Wextra
LDFLAGS ?=

SOURCES     := $(wildcard *.c)
LIB_SOURCES := $(filter-out mydb_main.c, $(SOURCES))
LIB_OBJECTS := $(LIB_SOURCES:.c=.o)
HEADERS     := $(wildcard *.h)

.PHONY: all lib clean

all: lib mydb_demo

lib: libmydb.a

libmydb.a: $(LIB_OBJECTS)
	$(AR) rcs $@ $^

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

mydb_demo: mydb_main.o libmydb.a
	$(CC) $(LDFLAGS) -o $@ $^

clean:
	rm -f *.o libmydb.a mydb_demo
