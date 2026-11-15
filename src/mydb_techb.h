#include "mydb_common.h"
#include "mydb_block.h"

#ifndef _MYDB_TECHB_H_
#define _MYDB_TECHB_H_
//-------------------------------------------------------------------------------------------------------------
/*   The bitmap of used data blocks: one bit per block,
 *   stored in its own pages between the file header and the data blocks.
 */
//-------------------------------------------------------------------------------------------------------------
eDBState techb_sync(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
void techb_destroy(IN sTechB *techb);
sTechB *techb_create(IN sDB *db, IN uchar_t *memory, IN uint_t offset);
//-------------------------------------------------------------------------------------------------------------
eDBState techb_set_bit(IN sDB *db, IN uint32_t offset, IN bool bit);
eDBState techb_get_bit(IN sDB *db, IN uint32_t offset, OUT bool *bit);
//-------------------------------------------------------------------------------------------------------------
uint32_t techb_get_index_of_first_free_bit(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_TECHB_H_
