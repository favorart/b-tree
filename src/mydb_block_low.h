#include "stdafx.h"
#include "mydb_block.h"
#include "mydb_techb.h"

#ifndef _MYDB_BLOCK_LOW_H_
#define _MYDB_BLOCK_LOW_H_
//-------------------------------------------------------------------------------------------------------------
eDBState block_read(IN sBlock *block);
eDBState block_seek(IN sBlock *block);
eDBState block_dump(IN sBlock *block);

/* write-back cache: only mark the page dirty, block_dump () writes it later */
static inline eDBState block_write(IN sBlock *block)
{
    block->dirty_ = true;
    return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/* !!! non-malloc'ed output */
eDBState block_change(IN sBlock *block, IN const sDBT *key, IN const sDBT *value, IN OUT sDBT *bkey,
                      IN uint_t Rptr);
eDBState block_insert(IN sBlock *block, IN const sDBT *key, IN const sDBT *value, OUT sDBT *bkey,
                      IN uint_t Rptr);
eDBState block_delete(IN sBlock *block, IN const sDBT *key, IN bool lptr);
//-------------------------------------------------------------------------------------------------------------
sDBT *block_key_next(IN sBlock *block, IN OUT sDBT *key, OUT uint_t *vsz);
sDBT block_key_data(IN sBlock *block, IN const sDBT *key);
#ifdef _DEBUG
int block_print_dbg(IN sBlock *block, IN const char *name);
#endif // _DEBUG
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_BLOCK_LOW_H_
