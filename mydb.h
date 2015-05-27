#include "stdafx.h"

#ifndef _MYDB_H_
#define _MYDB_H_

// #define _DEBUG
// #define  MYDB_NOCACHE  1
#define  MYDB_BITSINBYTE  8U
//-------------------------------------------------------------------------------------------------------------
typedef enum  db_state eDBState;
enum  db_state // : uchar_t
{ DONE = 0, FAIL = -1 };
//-------------------------------------------------------------------------------------------------------------
/* check `man dbopen` */
typedef struct DBT sDBT;
struct DBT
{
  void   *data;
  size_t  size;
};
//-------------------------------------------------------------------------------------------------------------
typedef struct DBC sDBC;
struct DBC
{
  /* Maximum on-disk file size
   * 512MB by default
   */
  size_t db_size;
  /* Page (node/data) size
   * 4KB by default
   */
  size_t page_size;
  /* Maximum cached memory size
   * 16MB by default
   */
  size_t cache_size;
};
//-------------------------------------------------------------------------------------------------------------
typedef struct DBFileHeader sDBFH;
struct DBFileHeader
{
  // Page        common;   /* общий */
  // PageSize    padding;  /* заполнение */
  //--------------------------------------
  uint32_t      db_size_;  /* const */ 
  uint32_t    page_size_;  /* const */
  uint32_t   cache_size_;  /* const */
  uint32_t  block_count_;  /* const */
  uint32_t  techb_count_;  /* const */
  uint32_t  nodes_count_;  /* меняем в block_index_free */
  uint32_t  offset2root_;  /* меняем при присвоении типа Root */
};
//-------------------------------------------------------------------------------------------------------------
typedef struct Block     sBlock;
typedef struct Block     sTechB;
typedef struct PageCache sCache;
typedef struct Logging   sLog;

typedef struct DB sDB;
struct DB
{
  //---------------------------------------------
  /* Public API */
  /* Returns 0 on OK, -1 on Error */
  int (*close)  (struct DB *db);
  int (*delete) (struct DB *db, const struct DBT *key);
  int (*insert) (struct DB *db, const struct DBT *key, const struct DBT *data);
  /* * * * * * * * * * * * * *
  *  Returns malloc'ed data into  'struct DBT *data'.
  *  Caller must free data->data. 'struct DBT *data'
  *  must be alloced in caller.
  * * * * * * * * * * * * * */
  int (*select) (struct DB *db, const struct DBT *key, struct DBT *data);
  /* Sync cached pages with disk */
  int (*sync)   (struct DB *db);
  /* For future uses - sync cached pages with disk
  * int (*sync)(const struct DB *db)
  */
  //---------------------------------------------
  /* Private API */
  HFILE    hfile_;
  sDBFH     head_;
  sBlock   *root_;

  sCache   *cache_;
  sLog     *log_;
  //---------------------------------------------
  sTechB   *techb_array_;
  uint_t    techb_last0_;
  //---------------------------------------------
  /*     ...     */
  //---------------------------------------------
}; /* Need for supporting multiple backends (HASH/BTREE) */
//-------------------------------------------------------------------------------------------------------------
sDB* dbcreate (const char *file, const sDBC *conf);
sDB* dbopen   (const char *file);

int  db_close  (sDB *);
int  db_delete (sDB *, void *, size_t);
int  db_select (sDB *, void *, size_t, void **, size_t *);;
int  db_insert (sDB *, void *, size_t, void *, size_t);
/* Syncronize the cached blocks with data on a disk */
int  db_flush  (sDB *);
//-------------------------------------------------------------------------------------------------------------
int  key_compare (IN const sDBT *k, IN const sDBT *key);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_H_
