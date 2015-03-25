#include "stdafx.h"
#include "mydb.h"

//-------------------------------------------------------------------------------------------------------------
int db_cls (sDB *db)
{ return db->close (db); }
int db_del (sDB *db, void *key, size_t key_len)
{
 struct DBT keyt = { .data = key,
                     .size = key_len };
 return db->delete (db, &keyt);
}
int db_get (sDB *db, void *key, size_t key_len, void **val, size_t *val_len)
{
 struct DBT keyt = { .data = key,
                     .size = key_len };
 struct DBT valt = { NULL, 0 };

 int rc   = db->select (db, &keyt, &valt);
 
 *val     = valt.data;
 *val_len = valt.size;
 return rc;
}
int db_put (sDB *db, void *key, size_t key_len, void  *val, size_t  val_len)
{
 struct DBT keyt = { .data = key,
                     .size = key_len };
 struct DBT valt = { .data = val,
                     .size = val_len };
 return db->insert (db, &keyt, &valt);
}
//-------------------------------------------------------------------------------------------------------------
