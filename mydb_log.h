#include "stdafx.h"
#include "mydb.h"

#ifndef _MYDB_LOG_H_
#define _MYDB_LOG_H_
//-------------------------------------------------------------------------------------------------------------
typedef enum
{ LOG_NONERC,
  LOG_INSERT, LOG_DELETE, LOG_REMOVE,
  LOG_DB_OPN, LOG_DB_CLS, LOG_CHKPNT
} mydb_log_enum;
//-------------------------------------------------------------------------------------------------------------
/* Открыть файл журнала на запись или чтение, и вернуть объект журнала */
sLog*  mydb_log_open  (IN bool trunc);
void   mydb_log_close (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
void   mydb_logging (IN sDB *db, IN mydb_log_enum type, IN const sDBT *key, IN const sDBT *val);
bool   mydb_recover (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOG_H_
