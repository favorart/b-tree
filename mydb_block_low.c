#include "stdafx.h"
#include "mydb_block_low.h"

//-------------------------------------------------------------------------------------------------------------
/*  function of iteration through the Block by keys
 *  to init the iteratio -, give a pointer to a key sDBT with zero size
 *  to stop the iteration - function returns NULL
 */
sDBT*     block_key_next (IN sBlock *block, IN       sDBT *key, OUT uint_t *vsz)
{
  if ( !(*block_nkvs (block)) )
    return NULL;

  if ( !key->size ) /* get first key */
  {
    uint_t* ksz = (uint_t*) block->data_;
    if ( vsz ) *vsz = *(ksz + 1U);

    key->data = (void*) (ksz + 2U);
    key->size = *ksz;
    return key;
  } // !!!
  else /* get next key */
  {
    uint_t  ptr_i = 0;
    uint_t  val_size = *((uint_t*) ((uchar_t*) key->data - sizeof (uint_t)));

    key->size = *((uchar_t*) key->data + key->size + val_size + sizeof (ptr_i));
    key->data = ((uchar_t*) key->data + key->size + val_size + sizeof (ptr_i)
                 + sizeof (key->size) + sizeof (val_size));

    if ( key->data >= (void*) block->free_ ) /* got all keys */
    {
      key->size = 0;
      key->data = NULL;
      if ( vsz ) *vsz = NULL;

      return NULL;
    }

    if ( vsz ) *vsz = ((uint_t*) key->data - 1);
    return key;
  } // end else
}
/*  return non-malloc'ed value array */
uint_t    block_key_data (IN sBlock *block, IN const sDBT *key, OUT void **data)
{
  if ( !key ) return 0;

  uint_t *vsz = (uint_t*) (key->data) - 1U;
  *data = ((uchar_t*) (key->data) + key->size);

  return *vsz;
}
#ifdef  DEBUG
int       block_print_data_debug (IN sBlock *block, IN const char *name)
{
  printf (">>> %s\n", name);
  printf ("type: %s\n", !((int*) block->memory_)[0] ? "Free" :
          ((int*) block->memory_)[0] == 1 ? "Pass" : "Leaf");
  printf ("nkvs: %d\n", ((int*) block->memory_)[1]);
  printf ("fset: %d\n", ((int*) block->memory_)[2]);
  printf ("free: %d\n", ((int*) block->memory_)[3]);
  for ( uint_t i = 4U; i < 5U * (*block_nkvs (block)) + 5U; ++i )
  {
    printf (" %x", ((int*) block->memory_)[i]);
    if ( !((i - 3) % 5) )
      printf ("\n");
  }
  printf ("\n\n");
  return 0;
}
#endif
//-------------------------------------------------------------------------------------------------------------
/*
*   В sBlock появятся дополнительные параметры:
*
*      sBlock *lru_next; — для участия в lru списке
*      sBlock *lru_prev;
*      enum { dirty, clean } status; — страница изменена и не записана на диск
*
*   Алгоритм block_read модифицируется следующим образом:
*
*      struct page *block_read (block_id_t block_no)
*      {
*        /* Search for a block in the cache by ID.
*         * If a block is found, put it first in the LRU-list, then return.
*         * Otherwise, maybe evict some blocks, read the block from a disk,
*         * insert into the hash, return block.
*         * /
*      }
*
*   Алгоритм block_write разбивается на 2 части:
*   1. Непосредственно в момент модификации данных, меняется статус страницы.
*      Если необходимо сделать вытеснение (eviction), и статус страницы == dirty,
*      вызывается старая реализация block_write.
*   2. Старая реализация block_write.
*
*   Рекомендуется сделать сначала LRU-кэш без поддержки журнала и отладить его.
*/
//-------------------------------------------------------------------------------------------------------------
/* LOW-LEVEL OPERATIONS */

eDBState  block_seek   (IN sBlock *block, IN bool mem)
{
  // if ( !block )
  // { /* just for beauty, to be able to set a position */
  //   return (lseek (block->owner_db_->hfile_, sizeof (sDBFH), SEEK_SET)
  //           == sizeof (sDBFH)) ? DONE : FAIL;
  // }
  //-----------------------------------------
  long offset = sizeof (sDBFH) + block->offset_
    * block->owner_db_->head_.page_size_;
  if ( mem )
    offset += block->owner_db_->head_.techb_count_
    * block->owner_db_->head_.page_size_;
  //-----------------------------------------
  /* lseek returns the offset, in bytes, of the new position from the beginning of the file */
  return (lseek (block->owner_db_->hfile_, offset, SEEK_SET) == offset) ? DONE : FAIL;
}
eDBState  block_read   (IN sBlock *block)
{
  //-----------------------------------------
  // long file_position = tell (block->owner_db_->hfile_);
  // if ( file_position == -1L )
  //   return FAIL;
  //-----------------------------------------
  if ( block->head_->db_offset_ >= block->owner_db_->head_.block_count_ )
    return FAIL;

  if ( DONE != block_seek (block, true) )
    return FAIL;

  if ( read (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
    return FAIL;

  // lseek (block->owner_db_->hfile_, file_position, SEEK_SET);
  //-----------------------------------------
  return DONE;
}
eDBState  block_write  (IN sBlock *block, IN bool mem)
{
  if ( DONE == block_seek (block, mem)
      && write (block->owner_db_->hfile_, block->memory_, block->size_) == block->size_ )
      return DONE;
  return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
eDBState  block_insert (IN sBlock *block, OUT sDBT *key, IN  const sDBT *value)
{
  //-----------------------------------------
  uchar_t *lay_mem = NULL;
  uint_t   need_free_size = (sizeof (uint_t) * 3 + value->size + key->size);
  //-----------------------------------------
  if ( need_free_size > block->head_->free_size_ )
    return FAIL;
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  {
    k = block_key_next (block, k, NULL);
  }
  //-----------------------------------------
  if ( k && !key_compare (k, key) )
  {
    sDBT val = { 0 };
    val.size = block_key_data (block, k, &val.data);
    if ( val.size != value->size )
    {
      // ptr to next key-value's
      lay_mem = ((uchar_t*) k->data + k->size + val.size);
      // move right or left
      memmove (lay_mem + (value->size - val.size), lay_mem, block->free_ - lay_mem);
      *((uint_t*) key->data - 1U) = value->size;
    }
    memcpy (val.data, value->data, val.size);

    /* CHANGING KEY */
    k->data = k->data;

    goto EXISTING;
  }
  else if ( k )
  {
    lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * 3);
    memmove (lay_mem + need_free_size, lay_mem, block->free_ - lay_mem);

     (*(uint_t*) lay_mem) = 0U; /* ptr */
     lay_mem += sizeof (uint_t);
  }
  else
  {
    lay_mem = block->free_;
    // need_free_size -= sizeof (uint_t); /* ptr */
  }
  //-----------------------------------------
  *(uint_t*) lay_mem =   key->size; lay_mem += sizeof (uint_t);
  *(uint_t*) lay_mem = value->size; lay_mem += sizeof (uint_t);
  //-----------------------------------------
  //-----------------------------------------
  memmove (lay_mem,               key->data,   key->size);
  memmove (lay_mem + key->size, value->data, value->size);

  /* CHANGING KEY */
  key->data = (void*) lay_mem;
  //-----------------------------------------
  block->free_ += need_free_size;
  block->head_->free_size_ -= need_free_size;
  //-----------------------------------------
  ++(*(block_nkvs (block)));
  //-----------------------------------------
EXISTING:;
#ifdef  DEBUG
  block_print_data_debug (block, "insert");
#endif
  return DONE;
}
eDBState  block_delete (IN sBlock *block, IN const sDBT *key)
{
  eDBState  result = FAIL;
  //-----------------------------------------
  uint_t vsz = 0;
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, &vsz);
  while ( k && key < k )
  {
    k = block_key_next (block, k, NULL);
  }
  //-----------------------------------------
  if ( k && !key_compare (key, k) )
  {
    uint_t  sz_mem = k->size + vsz + 3 * sizeof (uint_t);

    uchar_t *lay_mem = (uchar_t*) k->data - 3 * sizeof (uint_t);
    memcpy (lay_mem, lay_mem + sz_mem, sz_mem);

    block->free_ -= sz_mem;
    block->head_->free_size_ += sz_mem;
    //-----------------------------------------
    result = DONE;
  }
  //-----------------------------------------
  return result;
}
//-------------------------------------------------------------------------------------------------------------