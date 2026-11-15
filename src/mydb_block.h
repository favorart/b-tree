#include "stdafx.h"
#include "mydb_error.h"
#include "mydb.h"

#ifndef _MYDB_BLOCK_H_
#define _MYDB_BLOCK_H_

#define MYDB_OFFSET2NEW ((uint_t)(-1))
#define MYDB_INVALIDPTR ((uint_t)(-1))
/* ksz + vsz + right pointer, stored with every key-value pair */
#define MYDB_ENTRY_OVERHEAD (3U * sizeof(uint32_t))
//-------------------------------------------------------------------------------------------------------------
/* DB Node type */
typedef enum DBNT // : uchar_t
{
    Free = 0,
    Pass = 1,
    Leaf = 2 /* , Root=4 */
} eDBNT;
//-------------------------------------------------------------------------------------------------------------
/* Memory Block */
typedef struct DBHB sDBHB;
struct DBHB // db Block Header
{
    //----------------
    eDBNT node_type_;
    uint32_t kvs_count_;
    uint32_t db_offset_; /* (?const) offset of this Block in the db file */
    uint32_t free_size_; /* this variable must be less or equal 1/2 page_size_ */
    //----------------
    uint32_t pointer_0_; // kvs_count_ + 1
                         //----------------
};
//-------------------------------------------------------------------------------------------------------------
/*  | 4 bytes |  4 bytes  |   4 bytes   | key1.size | value1.size |  4 bytes | 4 bytes |
 *   pointer1 : key1.size : value1.size : key1.data : value1.data : pointer2 : offset2 = ptr1:sz:kv1:ptr2:fs1
 *                                                                             >> no limits
 *   +------+-------------------------------------------------------------------------------------------------+
 * v | head | ptr1, sz:kv1, ptr2, sz:kv2, ptr3, ... , ptrN, sz:kvN, ptr(N+1) -->                              |
 *   +------+-------------------------------------------------------------------------------------------------+
 *                                                                              v << limit
 *   +------+---------------------------+---------------------------------------------------------------------+
 * x | head | ptr1, ptr2, ..., ptr(N+1) | sz:kv1, sz:kv2, ... , sz:kvN -->      |      <-- fsN, ..., fs2, fs1 |
 *   +------+---------------------------+---------------------------------------------------------------------+
 *                                                                              ^                           */
//-------------------------------------------------------------------------------------------------------------
struct Block
{
    //----------------------
    /* the factual data */
    uchar_t *memory_;
    uint32_t size_;
    //----------------------
    /* (Block <--> Techb) interface */
    sDB *owner_db_;   /* pointer to owner date base - BTtree */
    uint32_t offset_; /* techb ipage, but for block doubles this->head->db_offset_ */
    bool dirty_;      /* modified, not written to disk yet */
    bool is_mem_;
    //----------------------
    sDBHB *head_;   /* pointer to interpret the begining of memory_ as aBlock header */
    uchar_t *data_; /* pointer to memory after pointers array */
    uchar_t *free_; /* pointer to memory after all useful data in node */
    //----------------------
    uint_t lsn_;
};
//-------------------------------------------------------------------------------------------------------------
bool block_isfull(IN const sBlock *block);
//-------------------------------------------------------------------------------------------------------------
eDBNT *block_type(IN sBlock *block);
uint_t *block_nkvs(IN sBlock *block);
/* child pointers: left of key (key == NULL: the very right one) and right of key (key == NULL: the very left one) */
uint32_t block_lptr(IN sBlock *block, IN const sDBT *key);
uint32_t block_rptr(IN sBlock *block, IN const sDBT *key);
/* largest entry (MYDB_ENTRY_OVERHEAD + key + value) a block of this db accepts */
uint32_t block_max_entry(IN const sDB *db);
//-------------------------------------------------------------------------------------------------------------
/* Blocks are owned by the page cache: there is nothing to free */
sBlock *block_create(IN sDB *db, IN uint_t offset);
/* Return the block to the free list (bitmap) */
void block_release(IN sDB *db, IN uint32_t offset);
//-------------------------------------------------------------------------------------------------------------
eDBState block_add_nonfull(IN sBlock *block, IN const sDBT *key, IN const sDBT *value);
eDBState block_select_deep(IN sBlock *block, IN const sDBT *key, OUT sDBT *value);
int block_delete_deep(IN sBlock *block, IN const sDBT *key);
//-------------------------------------------------------------------------------------------------------------
/* key - the key being inserted (sequential inserts split at the edge) or NULL (in two halves) */
eDBState block_split_child(IN sBlock *parent, IN sBlock *ychild, OUT sBlock *zchild, IN const sDBT *key);
/* check the tree invariants: number of keys or -1 */
long block_check_tree(IN sDB *db);
/* the page statistics of the tree, 0 or -1 */
int block_stats(IN sDB *db, OUT sDBStat *st);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_BLOCK_H_
