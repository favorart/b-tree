#include "stdafx.h"
#include "mydb.h"
#include "mydb_block.h"
#include "mydb_block_low.h"

#ifndef _MYDB_CACHE_H_
#define _MYDB_CACHE_H_
//-------------------------------------------------------------------------------------------------------------
typedef uint32_t hash_t;
typedef struct Cache_CyclicHashTable_BuldInList sCchHL;
/* LRU cache с поддержкой write-back позволяет 
 * отложенно записывать изменения на диск.
 */
typedef struct PageCache sCache;
struct PageCache
{
  //-----------------------------------------
  uint_t      n_pgs;
  uint_t    all_pgs;
  uint_t     sz_pg;
  //-----------------------------------------
  /* Quickly lookup a page by id */
  uint_t    szHash_;
  sCchHL   *blHash_; // malloced
  //-----------------------------------------
  /* Double-linked list of pages */
  sCchHL   *LRUHead;
  sCchHL   *LRULast;
  //-----------------------------------------
  sBlock   *blocks_; // malloced
  uchar_t  *memory_; // malloced
  uint_t    m_size_;
  //-----------------------------------------
};

bool  mydb_cache_init (IN sDB *db);
bool  mydb_cache_push (IN sDB *db, IN uint32_t offset, OUT sBlock **block);
void  mydb_cache_sync (IN sDB *db);
void  mydb_cache_fine (IN sDB *db);
void  mydb_cache_free (IN sDB *db);

#ifdef _DEBUG_CACHE
void  mydb_cache_print_debug (IN sDB *db);
#endif // _DEBUG_CACHE
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_CACHE_H_
