#ifndef _STDAFX_H_
#define _STDAFX_H_
//-------------------------------------------------------------------------------------------------------------
#include <sys\stat.h>
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
/* linux code goes here */
#include <unistd.h>
#elif _WIN32
#pragma warning (disable : 4996) // safe-windows functions
#pragma warning (disable : 4047) // pointer to function in a return

/*  Values for the second argument to access */
#define F_OK  0  /* Test for existence.  */
#define W_OK  2  /* Test for write permission.  */
#define R_OK  4  /* Test for read permission.  */
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
typedef enum mydb_error e_mydb_err;
enum mydb_error
{ 
  MYDB_ERR_NONE,   MYDB_ERR_FPARAM, MYDB_ERR_FNEXST, 
  MYDB_ERR_BWRITE, MYDB_ERR_OFFSET, // MYDB_ERR_
  /* , ... */
} ;

e_mydb_err mydb_errno;
const char* strmyerror (e_mydb_err err);
//-------------------------------------------------------------------------------------------------------------
#endif // _STDAFX_H_
