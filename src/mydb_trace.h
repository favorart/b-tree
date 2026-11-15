#include "stdafx.h"
#include "mydb.h"

#ifndef _MYDB_TRACE_H_
#define _MYDB_TRACE_H_
//-------------------------------------------------------------------------------------------------------------
/*  TRACE: a text log of what the tree is doing, to debug without a debugger.
 *
 *  Enabled by db_trace () or by the environment on dbcreate () / dbopen ():
 *    MYDB_TRACE=1..4       the level (MYDB_TRACE_OPS .. MYDB_TRACE_DUMP)
 *    MYDB_TRACE_FILE=path  append to a file instead of stderr
 *
 *  Every line starts with the number of the API operation, so all the
 *  actions of one insert/delete/select can be grepped together:
 *
 *    mydb #42 insert key="apple" (5 B) value 8 B
 *    mydb #42   page 3 inner, 12 keys, 1840 B free -> child page 17
 *    mydb #42   split page 17 -> new page 31, median "kiwi", up into page 3
 *    mydb #42   page 31 leaf: insert
 *    mydb #42 insert done
 *
 *  With the trace off a trace point costs one comparison of an int.
 */
//-------------------------------------------------------------------------------------------------------------
extern int mydb_trace_level;

/* read MYDB_TRACE / MYDB_TRACE_FILE once, unless db_trace () was called */
void mydb_trace_init(void);
/* start of an API operation: the next operation number */
void mydb_trace_op(void);

#if defined(__GNUC__)
__attribute__((format(printf, 2, 3)))
#endif
void mydb_trace_printf(IN int level, IN const char *fmt, ...);

#define MYDB_TRACE_KEYBUF 80U
/* a printable form of a key: "text" or hex, cut to fit the buffer */
const char *mydb_trace_key(IN const void *data, IN size_t size, OUT char buf[MYDB_TRACE_KEYBUF]);

#define TRACE(level, ...)                                                                                    \
    do                                                                                                       \
    {                                                                                                        \
        if (mydb_trace_level >= (level))                                                                     \
            mydb_trace_printf((level), __VA_ARGS__);                                                         \
    } while (0)
#define TRACE_ON(level) (mydb_trace_level >= (level))
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_TRACE_H_
