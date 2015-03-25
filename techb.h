#include "stdafx.h"
#include "mydb.h"

#ifndef _TECHB_H_
#define _TECHB_H_
//-------------------------------------------------------------------------------------------------------------
typedef enum  tch_blk_state eTBState;
enum  tch_blk_state // : uchar_t
{ FREE, NODE };
//-------------------------------------------------------------------------------------------------------------
typedef struct TechB sTechB;
struct TechB
{
 //----------------------
 uchar_t  *memory_;
 uint32_t    size_;
 uint32_t  offset_; // ipage
 bool       dirty_;
 //----------------------
};

eDBState   techb_sync    (IN sDB     *db);
void       techb_destroy (IN sTechB  *techb);
sTechB*    techb_create  (IN sDB     *db,
                          IN uchar_t *memory,
                          IN uint_t   offset);


//-------------------------------------------------------------------------------------------------------------
#endif // _TECHB_H_
