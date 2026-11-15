#include "mydb_common.h"
#include "mydb_low.h"
#include "mydb.h"
#include "mydb_error.h"
#include "mydb_block.h"

//-------------------------------------------------------------------------------------------------------------
/* Returns -1 = (this < that); 0 = (this == that); 1 = (this > that)  */
int key_compare(IN const sDBT *this_key, IN const sDBT *that_key)
{
    size_t min_size = (this_key->size < that_key->size) ? this_key->size : that_key->size;
    int res = memcmp(this_key->data, that_key->data, min_size);

    if (res)
        return (res < 0) ? -1 : 1;
    return (this_key->size < that_key->size) ? -1 : (this_key->size > that_key->size) ? 1 : 0;
}
//-------------------------------------------------------------------------------------------------------------
sDB *dbcreate(const char *file, IN const sDBC *conf)
{
    return mydb_create(file, conf);
}
sDB *dbopen(const char *file)
{
    return mydb_open(file);
}
//-------------------------------------------------------------------------------------------------------------
int db_close(sDB *db)
{
    return (db) ? db->close(db) : 0;
}
/* a NULL handle or a NULL out parameter: -1 and MYDB_ERR_FPARAM, not a crash */
static int mydb_bad_args(void)
{
    mydb_errno = MYDB_ERR_FPARAM;
    return -1;
}
int db_delete(sDB *db, void *key, size_t key_len)
{
    if (!db)
        return mydb_bad_args();
    struct DBT keyt = {.data = key, .size = key_len};
    return db->delete (db, &keyt);
}
int db_select(sDB *db, void *key, size_t key_len, void **val, size_t *val_len)
{
    if (!db || !val || !val_len)
        return mydb_bad_args();
    struct DBT keyt = {.data = key, .size = key_len};
    struct DBT valt = {NULL, 0};

    int rc = db->select(db, &keyt, &valt);
    if (rc)
    {
        valt.data = NULL;
        valt.size = 0;
    }

    *val = valt.data;
    *val_len = valt.size;
    return rc;
}
int db_insert(sDB *db, void *key, size_t key_len, void *val, size_t val_len)
{
    if (!db)
        return mydb_bad_args();
    struct DBT keyt = {.data = key, .size = key_len};
    struct DBT valt = {.data = val, .size = val_len};
    return db->insert(db, &keyt, &valt);
}
//-------------------------------------------------------------------------------------------------------------
int db_flush(sDB *db)
{
    return db ? mydb_flush(db) : mydb_bad_args();
}
//-------------------------------------------------------------------------------------------------------------
long db_check(sDB *db)
{
    return db ? block_check_tree(db) : mydb_bad_args();
}
//-------------------------------------------------------------------------------------------------------------
int db_stats(sDB *db, sDBStat *st)
{
    if (!db || !st)
        return mydb_bad_args();
    return block_stats(db, st);
}
//-------------------------------------------------------------------------------------------------------------
#ifndef MYDB_LIB_VERSION
#define MYDB_LIB_VERSION "unknown" /* set by the Makefile from the git tag */
#endif
const char *db_version(void)
{
    return MYDB_LIB_VERSION;
}
//-------------------------------------------------------------------------------------------------------------
