/*  Common definitions of the implementation (not a part of the public API). */
#ifndef _MYDB_COMMON_H_
#define _MYDB_COMMON_H_
//-------------------------------------------------------------------------------------------------------------
#if !defined(__unix__) && !defined(__APPLE__)
#error mydb needs a POSIX system
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
//-------------------------------------------------------------------------------------------------------------
/* markers of the input and output parameters */
#define IN
#define OUT
//-------------------------------------------------------------------------------------------------------------
typedef unsigned char uchar_t;
typedef uint32_t uint_t;
//-------------------------------------------------------------------------------------------------------------
/* Unaligned-safe access to the 32-bit fields packed inside a page */
static inline uint32_t mydb_rd32(const void *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}
static inline void mydb_wr32(void *p, uint32_t v)
{
    memcpy(p, &v, sizeof(v));
}
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_COMMON_H_
