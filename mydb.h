#include "stdafx.h"

#ifndef _MYDB_H_
#define _MYDB_H_
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
{                                       // Page        common;  // общий
 /* const */ uint32_t      db_size_;
 /* const */ uint32_t    page_size_;  
             uint32_t  offset2root_;
             uint32_t  nodes_count_;    // PageSize    padding; // заполнение    
 /* const */ uint32_t  block_count_;
 /* const */ uint32_t  techb_count_;
};
//-------------------------------------------------------------------------------------------------------------
typedef struct Block sBlock;
typedef struct Block sTechB;

typedef struct DB sDB; 
struct DB
{
  //---------------------------------------------
	/* Public API */
	/* Returns 0 on OK, -1 on Error */
	int  (*close)  (struct DB *db);
	int  (*delete) (struct DB *db, const struct DBT *key);
	int  (*insert) (struct DB *db, const struct DBT *key, const struct DBT *data);
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
  //---------------------------------------------
  sTechB   *techb_arr_;
  size_t    techb_last_free;
  //---------------------------------------------  
  sBlock   *extra_; 
  sBlock   *child_; 
  sBlock   *parent_;

  sBlock  **path_;
  /*     ...     */
  //---------------------------------------------
}; /* Need for supporting multiple backends (HASH/BTREE) */
//-------------------------------------------------------------------------------------------------------------
/* Open DB if it exists, otherwise create DB */
sDB* db_open (const char *file, const sDBC *conf);

int  db_cls (sDB *);
int  db_del (sDB *, void *, size_t);
int  db_get (sDB *, void *, size_t, void **, size_t *);
int  db_put (sDB *, void *, size_t, void * , size_t  );
/* Sync cached pages with disk */
int  db_sync (sDB *db);
//-------------------------------------------------------------------------------------------------------------
int  key_compare (IN const sDBT *key, IN const sDBT *k);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_H_