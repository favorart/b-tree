#include "stdafx.h"

#ifndef _MYDB_ERROR_H_
#define _MYDB_ERROR_H_
//-------------------------------------------------------------------------------------------------------------
typedef enum mydb_error e_mydb_err;
enum mydb_error
{
  MYDB_ERR_NONE,   MYDB_ERR_FPARAM, MYDB_ERR_FNEXST,
  MYDB_ERR_BWRITE, MYDB_ERR_OFFSET, MYDB_ERR_NFREEB,
  MYDB_ERR_NFREES, MYDB_ERR_CCHSET /* , ... */
};

e_mydb_err mydb_errno;
const char*  strmyerror (e_mydb_err err);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_ERROR_H_