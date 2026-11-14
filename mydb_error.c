#include "stdafx.h"
#include "mydb_error.h"

//-------------------------------------------------------------------------------------------------------------
e_mydb_err mydb_errno = MYDB_ERR_NONE;
//-------------------------------------------------------------------------------------------------------------
const char* strmyerror (e_mydb_err err)
{
  const char* strerr;
  //-----------------------------------------
  switch ( err )
  { default: strerr = NULL; break;

    case MYDB_ERR_FPARAM : strerr = ": Incorrect parameter to function."; break;
    case MYDB_ERR_FNEXST : strerr = ": mydb file does not exist on disk."; break;
    case MYDB_ERR_BWRITE : strerr = ": Writing block to disk failed."; break;
    case MYDB_ERR_OFFSET : strerr = ": Invalid offset for disk action."; break;
    case MYDB_ERR_NFREEB : strerr = ": No free block for node in file."; break;
    case MYDB_ERR_NFREES : strerr = ": No free space for key in block."; break;
    case MYDB_ERR_CCHSET : strerr = ": Fault inserting block to cache."; break;
    case MYDB_ERR_KVSIZE : strerr = ": Key-value pair is too large.";     break;
    case MYDB_ERR_FORMAT : strerr = ": Not a mydb file (bad header).";    break;
    case MYDB_ERR_LOGWRT : strerr = ": Writing the journal failed.";      break;
  }
  //-----------------------------------------
  mydb_errno = MYDB_ERR_NONE;
  return strerr;
}
//-------------------------------------------------------------------------------------------------------------
