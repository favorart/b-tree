#include "stdafx.h"
#include "mydb_block.h"

#ifndef _MYDB_TECHB_H_
#define _MYDB_TECHB_H_
//-------------------------------------------------------------------------------------------------------------
/*   N - file size; K - number of blocks
 *   K * 1 bit = M - technical information - the bitmask of free blocks
 *  (N - M) - the space for storing data
 */
//-------------------------------------------------------------------------------------------------------------
typedef enum tch_blk_state eTBState;
enum tch_blk_state // : uchar_t
{
    FREE,
    NODE
};
//-------------------------------------------------------------------------------------------------------------
typedef sBlock sTechB;
//-------------------------------------------------------------------------------------------------------------
eDBState techb_sync(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
void techb_destroy(IN sTechB *techb);
sTechB *techb_create(IN sDB *db, IN uchar_t *memory, IN uint_t offset);
//-------------------------------------------------------------------------------------------------------------
eTBState techb_set_bit(IN sDB *db, IN uint32_t offset, IN bool bit);
eTBState techb_get_bit(IN sDB *db, IN uint32_t offset, OUT bool *bit);
//-------------------------------------------------------------------------------------------------------------
uint32_t techb_get_index_of_first_free_bit(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_TECHB_H_
