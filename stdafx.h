#ifndef _STDAFX_H_
#define _STDAFX_H_
//-------------------------------------------------------------------------------------------------------------
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <io.h>
//-------------------------------------------------------------------------------------------------------------
#ifdef __linux__ 
// linux code goes here
#elif _WIN32
#pragma warning (disable:4996)
#else
#error Platform not supported
#endif

#define IN
#define OUT
//-------------------------------------------------------------------------------------------------------------
typedef int              HFILE;
typedef unsigned char  uchar_t;
typedef uint32_t        uint_t;
typedef uint64_t       ulong_t;
//-------------------------------------------------------------------------------------------------------------
#endif // _STDAFX_H_
