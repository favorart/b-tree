#include "mydb_common.h"
#include "mydb.h"

#ifndef _MYDB_ERROR_H_
#define _MYDB_ERROR_H_
//-------------------------------------------------------------------------------------------------------------
extern e_mydb_err mydb_errno;
const char *strmyerror(e_mydb_err err);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_ERROR_H_
