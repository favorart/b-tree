#include "stdafx.h"
#include "mydb_error.h"
//-------------------------------------------------------------------------------------------------------------
const char* strmyerror (e_mydb_err err)
{
  const char* strerr;
  //-----------------------------------------
  switch ( err )
  { default: strerr = NULL; break;

    case MYDB_ERR_FPARAM : ": Incorect parameter to function."; break;
    case MYDB_ERR_FNEXST : ": mydb file is not exist on disk."; break;
    case MYDB_ERR_BWRITE : ": Writing block to disk is fault."; break;
    case MYDB_ERR_OFFSET : ": Invalid offset for disk action."; break;
  }
  //-----------------------------------------
  mydb_errno = MYDB_ERR_NONE;
  return strerr;
}
//-------------------------------------------------------------------------------------------------------------
