#include "stdafx.h"
#include "block.h"

#ifndef _TECHB_H_
#define _TECHB_H_
//-------------------------------------------------------------------------------------------------------------
typedef enum  tch_blk_state eTBState;
enum  tch_blk_state // : uchar_t
{ FREE, NODE };
//-------------------------------------------------------------------------------------------------------------
typedef sBlock sTechB;
//-------------------------------------------------------------------------------------------------------------
eDBState   techb_sync    (IN sDB     *db);
//-------------------------------------------------------------------------------------------------------------
void       techb_destroy (IN sTechB *techb);
sTechB*    techb_create  (IN sDB     *db,
                          IN uchar_t *memory,
                          IN uint_t   offset);
//-------------------------------------------------------------------------------------------------------------
uint32_t   techb_get_bit (IN sDB *db, IN uint32_t offset, IN bool first_free);
eTBState   techb_set_bit (IN sDB *db, IN uint32_t offset, IN bool bit);
//-------------------------------------------------------------------------------------------------------------
#endif // _TECHB_H_
