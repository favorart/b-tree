#ifndef _STDAFX_H_
#define _STDAFX_H_
//-------------------------------------------------------------------------------------------------------------
#ifdef __linux__ 
/* linux code goes here */
#define __USE_MISC      1
#define _XOPEN_SOURCE   700

#include <unistd.h>

#define S_IREAD         S_IRUSR
#define S_IWRITE        S_IWUSR
#define S_IEXEC         S_IXUSR
 /* Under UNIX there is no difference
  * between text and binary files.
  */
#define O_BINARY        0
#elif _WIN32
#pragma warning (disable : 4996) // safe-windows functions
#pragma warning (disable : 4047) // pointer to function in a return

/*  Values for the second argument to access */
#define F_OK  0  /* Test for existence.  */
#define W_OK  2  /* Test for write permission.  */
#define R_OK  4  /* Test for read permission.  */

#include <io.h>
#else
#error Platform not supported
#endif
//-------------------------------------------------------------------------------------------------------------
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <time.h>
//-------------------------------------------------------------------------------------------------------------
#define IN
#define OUT
//-------------------------------------------------------------------------------------------------------------
typedef int              HFILE;
typedef unsigned char  uchar_t;
typedef unsigned int    uint_t;
//-------------------------------------------------------------------------------------------------------------
#define SWAP(X,Y) \
    do { unsigned char _buf[sizeof (*(X))]; \
         memmove(_buf, (X), sizeof (_buf)); \
         memmove((X),  (Y), sizeof (_buf)); \
         memmove((Y), _buf, sizeof (_buf)); \
       } while (0)
//-------------------------------------------------------------------------------------------------------------
#endif // _STDAFX_H_
