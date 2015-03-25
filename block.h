#include "stdafx.h"
#include "mydb.h"

#ifndef _BLOCK_H_
#define _BLOCK_H_

//-------------------------------------------------------------------------------------------------------------
/* db node type */
typedef enum DBNT eDBNT;
enum DBNT // : uchar_t
{ Free, Root, Pass, Leaf };
//-------------------------------------------------------------------------------------------------------------
/* struct  of a  block */
/* struct Memory Block */
typedef struct DBHB sDBHB;
struct DBHB // db Block Header
{
 //----------------
 eDBNT     node_type_;
 uint32_t  kvs_count_;
 uint32_t  db_offset_; /* (?const) offset of this Block in the db file */
 uint32_t  free_size_; /* this variable must be less or equal 1/2 page_size_ */ 
 //----------------
 uint32_t  pointer_0_; // kvs_count_ + 1
 //----------------
};
//-------------------------------------------------------------------------------------------------------------
/*
 * Блок
 * typedef struct NodeHeader head; - сколько ключей в блоке, тип блока, ...
 *
 *  |  4 bytes  |  4 bytes  |   4 bytes   | key1.size | value1.size |   4 bytes  | 4 bytes |
 *   pointer(1) : key1.size : value1.size : key1.data : value1.data : pointer(2) : offset2 = ptr1:sz:kv1:ptr2:fs1
 *                                                                             >> no limits
 *   +------+-------------------------------------------------------------------------------------------------+
 * v | head | ptr1, sz:kv1, ptr2, sz:kv2, ptr3, ... , ptr(N), sz:kvN, ptr(N+1) -->                            |
 *   +------+-------------------------------------------------------------------------------------------------+
 *                                                                              v << limit
 *   +------+---------------------------+---------------------------------------------------------------------+
 * x | head | ptr1, ptr2, ..., ptr(N+1) | sz:kv1, sz:kv2, ... , sz:kv(N) -->    |    <-- fs(N), ..., fs2, fs1 |
 *   +------+---------------------------+---------------------------------------------------------------------+
 *                                                                              ^
 */
//-------------------------------------------------------------------------------------------------------------
typedef struct Block sBlock;
struct Block
{
 //----------------------
 /* the factual data */
 uchar_t  *memory_;
 uint32_t  size_;
 //----------------------
 sDB      *owner_db_; /* pointer to owner date base - BTtree */
 sDBHB    *head_;     /* pointer to interpret the begining of memory_ as aBlock header */
 uchar_t  *data_;     /* pointer to memory after pointers array */
 //----------------------
};
//-------------------------------------------------------------------------------------------------------------
bool       block_is_full (IN sBlock *block);
//-------------------------------------------------------------------------------------------------------------
eDBNT*     block_type (IN sBlock *block);
uint_t*    block_nkvs (IN sBlock *block);
//-------------------------------------------------------------------------------------------------------------
eDBState   block_insert  (IN sBlock *block, IN sDBT *key, IN  sDBT *value);
eDBState   block_select  (IN sBlock *block, IN sDBT *key, OUT sDBT *value);
eDBState   block_delete  (IN sBlock *block, IN sDBT *key);
//-------------------------------------------------------------------------------------------------------------
eDBState   block_write   (IN sBlock *block);
void       block_destroy (IN sBlock *block);
sBlock*    block_create  (IN sDB    *db, IN uint_t offset);
//-------------------------------------------------------------------------------------------------------------
#endif // _BLOCK_H_
