#include "stdafx.h"
#include "mydb_block.h"
#include "mydb_techb.h"

#ifndef _MYDB_BLOCK_LOW_H_
#define _MYDB_BLOCK_LOW_H_

// #define _DEBUG
//-------------------------------------------------------------------------------------------------------------
eDBState  block_read (IN sBlock *block);
eDBState  block_seek (IN sBlock *block);
eDBState  block_dump (IN sBlock *block);

#ifdef MYDB_NOCACHE 
#define  block_write  block_dump
#else // MYDB_NOCACHE
#define  block_write
#endif
//-------------------------------------------------------------------------------------------------------------
/* !!! non-malloc'ed output */
eDBState  block_select (IN sBlock *block, IN const sDBT *key, OUT      sDBT *value, OUT    sDBT *bkey);
eDBState  block_change (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, IN OUT sDBT *bkey, IN uint_t Rptr);
eDBState  block_insert (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, OUT    sDBT *bkey, IN uint_t Rptr);
eDBState  block_delete (IN sBlock *block, IN const sDBT *key, IN bool lptr);
//-------------------------------------------------------------------------------------------------------------
sDBT*     block_key_next (IN sBlock *block, IN OUT   sDBT *key, OUT uint_t *vsz);
sDBT      block_key_data (IN sBlock *block, IN const sDBT *key);

#ifdef _DEBUG
int       block_print_dbg (IN sBlock *block, IN const char *name);
#endif // _DEBUG
//-------------------------------------------------------------------------------------------------------------
bool  block_recursively_delete_key_in_left_branch (IN sBlock *parent, IN sBlock *ychild, IN const sDBT *key);
bool  block_recursively_delete_key_in_rght_branch (IN sBlock *parent, IN sBlock *zchild, IN const sDBT *key);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_BLOCK_LOW_H_
