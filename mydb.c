#include "stdafx.h"
#include "mydb.h"


//-------------------------------------------------------------------------------------------------------------
/* Returns -1 = (this > that); 0 = (this == that); 1 = (this < that)  */
int  key_compare (IN const sDBT *this_key, IN const sDBT *that_key)
{ // if ( this_key->size != that_key->size )
  //  return (this_key->size > that_key->size) ? 1 : -1;
  int res = memcmp (this_key->data, that_key->data, // this_key->size);
                    (this_key->size <= that_key->size) ?
                    this_key->size : that_key->size);
  return res;
}
//-------------------------------------------------------------------------------------------------------------
sDB* dbcreate  (const char *file, IN const sDBC *conf)
{ return mydb_create (file, conf); }
sDB* dbopen    (const char *file)
{ return mydb_open (file); }
//-------------------------------------------------------------------------------------------------------------
int  db_close  (sDB *db)
{ return (db) ? db->close (db) : 0; }
int  db_delete (sDB *db, void *key, size_t key_len)
{
  struct DBT keyt = { .data = key,
    .size = key_len };
  return db->delete (db, &keyt);
}
int  db_select (sDB *db, void *key, size_t key_len, void **val, size_t *val_len)
{
  struct DBT keyt = { .data = key,
    .size = key_len };
  struct DBT valt = { NULL, 0 };

  int rc = db->select (db, &keyt, &valt);

  *val = valt.data;
  *val_len = valt.size;
  return rc;
}
int  db_insert (sDB *db, void *key, size_t key_len, void  *val, size_t  val_len)
{
  struct DBT keyt = { .data = key,
    .size = key_len };
  struct DBT valt = { .data = val,
    .size = val_len };
  return db->insert (db, &keyt, &valt);
}
//-------------------------------------------------------------------------------------------------------------
int  db_flush  (sDB *db)
{ return mydb_flush (db); }
//-------------------------------------------------------------------------------------------------------------
