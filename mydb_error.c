#include "stdafx.h"
#include "mydb_error.h"
//-------------------------------------------------------------------------------------------------------------
const char* strmyerror (e_mydb_err err)
{
  const char* strerr;
  //-----------------------------------------
  switch ( err )
  { default: strerr = NULL; break;

    case MYDB_ERR_FPARAM : strerr = ": Incorect parameter to function."; break;
    case MYDB_ERR_FNEXST : strerr = ": mydb file is not exist on disk."; break;
    case MYDB_ERR_BWRITE : strerr = ": Writing block to disk is fault."; break;
    case MYDB_ERR_OFFSET : strerr = ": Invalid offset for disk action."; break;
    case MYDB_ERR_NFREEB : strerr = ": No free block for node in file."; break;
    case MYDB_ERR_NFREES : strerr = ": No free space for key in block."; break;
  }
  //-----------------------------------------
  mydb_errno = MYDB_ERR_NONE;
  return strerr;
}
//-------------------------------------------------------------------------------------------------------------
