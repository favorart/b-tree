#include "mydb_common.h"
#include "mydb.h"
#include "mydb_block.h"
#include "mydb_block_low.h"

#ifndef _MYDB_CACHE_H_
#define _MYDB_CACHE_H_
//-------------------------------------------------------------------------------------------------------------
typedef uint32_t hash_t;
typedef struct Cache_CyclicHashTable_BuildInList sCchHL;
/* LRU cache with write-back:
 * changes are written to disk later.
 */
struct PageCache
{
    //-----------------------------------------
    uint_t n_pgs;
    uint_t all_pgs;
    uint_t sz_pg;
    //-----------------------------------------
    /* Quickly lookup a page by id */
    uint_t szHash_;
    sCchHL *blHash_; // malloced
    //-----------------------------------------
    /* Double-linked list of pages */
    sCchHL *LRUHead;
    sCchHL *LRULast;
    //-----------------------------------------
    sBlock *blocks_;  // malloced
    uchar_t *memory_; // malloced
    uint_t m_size_;
    //-----------------------------------------
};

bool mydb_cache_init(IN sDB *db);
bool mydb_cache_push(IN sDB *db, IN uint32_t offset, OUT sBlock **block);
/* write all dirty pages, true on error */
bool mydb_cache_sync(IN sDB *db);
/* every cached page and the bitmap are written on the next sync (after a failed fsync) */
void mydb_cache_mark_dirty(IN sDB *db);
void mydb_cache_free(IN sDB *db);

//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_CACHE_H_
