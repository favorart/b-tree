#include "stdafx.h"

#ifndef _MYDB_H_
#define _MYDB_H_
//-------------------------------------------------------------------------------------------------------------
#ifdef _WIN32
#pragma warning (disable : 4047) // pointer to function in a return
#endif

#define BITSINBYTE    8U
#define MAXKEYS      (2U*MINKEYS - 1U)
#define MINKEYS      (16U)
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
 /* const */ uint32_t      db_size_;    // uint32_t    db_size;
 /* const */ uint32_t    page_size_;    // PageSize    page_size;
                                        // const uint32_t  cache_size_;    
             uint32_t  offset2root_;    // PageNumber  root;
             uint32_t  nodes_count_;    // PageSize    padding; // заполнение    
 /* const */ uint32_t  block_count_;
 /* const */ uint32_t  techb_count_;    // PageSize    index_count;
                                        // PageNumber  index_number[1];
};
//-------------------------------------------------------------------------------------------------------------
typedef struct TechB sTechB;
typedef struct Block sBlock;

typedef struct DB sDB; 
struct DB
{
	/* Public API */
	/* Returns 0 on OK, -1 on Error */
	int  (*close)  (struct DB *db);
	int  (*delete) (struct DB *db, struct DBT *key);
	int  (*insert) (struct DB *db, struct DBT *key, struct DBT *data);
	/* * * * * * * * * * * * * *
	 * Returns malloc'ed data into 'struct DBT *data'.
	 * Caller must free data->data.'struct DBT *data'
  * must be alloced in caller.
	 * * * * * * * * * * * * * */
	int (*select) (struct DB *db, struct DBT *key, struct DBT *data);
	/* Sync cached pages with disk */
	int (*sync)   (struct DB *db);
	/* For future uses - sync cached pages with disk
	 * int (*sync)(const struct DB *db)
	 */

	/* Private API */
 HFILE    hfile_;
 sDBFH     head_;
 
 sTechB   *techb_arr_;

 sBlock   *root_;
 sBlock   *extra_;   // uchar_t  *extra_block_;
 sBlock   *child_;   // uchar_t  *child_block_;
 sBlock   *parent_;  // uchar_t  *parnt_block_;
 /*     ...     */

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
int  key_compare (sDBT *key, sDBT *k);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_H_