sources := $(wildcard *.c)
objects := $(addsuffix .o, $(basename $(sources)))
lib_sources := $(filter-out mydb_main.c, ${sources}) 
lib_objects := $(addsuffix .o, $(basename $(lib_sources)))
cc := gcc
cflags := -g -ggdb -O0 -std=c99

%.o: %.c
	$(cc) $(cflags) -c $< -o $@
a.out:  ${objects}
	$(cc) $? -O0 -ggdb -std=c99 -o $@

lib: ${lib_objects}
	ar cru $? -o libmydb.a

clean:
	rm -f ${objects} a.out
