#include "stdafx.h"
#include "mydb.h"

#ifndef _MYDB_LOW_H_
#define _MYDB_LOW_H_
//-------------------------------------------------------------------------------------------------------------
/* INTERFACE */

sDB* mydb_create (IN const char *file, IN const sDBC *conf);
sDB* mydb_open   (IN const char *file);

int  mydb_close  (IN sDB *db);
int  mydb_delete (IN sDB *db, IN const sDBT *key);
int  mydb_insert (IN sDB *db, IN const sDBT *key, IN  const sDBT *data);
int  mydb_select (IN sDB *db, IN const sDBT *key, OUT       sDBT *data);
int  mydb_flush  (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
int  mydb_head_sync (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOW_H_
