#include "stdafx.h"
#include "mydb_block_low.h"

//-------------------------------------------------------------------------------------------------------------
/*  function of iteration through the Block by keys
 *  to init the iteratio -, give a pointer to a key sDBT with zero size
 *  to stop the iteration - function returns NULL
 */
sDBT*     block_key_next  (IN sBlock *block, IN OUT   sDBT *key, OUT uint_t *vsz)
{
  if ( !key || (key->size && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_)) )
  { fprintf (stderr, "memory block key next%s", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }
  
  if ( *block_nkvs (block) <= 0 )
    return NULL;

  if ( !key->size ) /* get first key */
  {
    uint_t* ksz = (uint_t*) block->data_;
    if ( vsz ) *vsz = *(ksz + 1U);

    key->data = (void*) (ksz + 2U);
    key->size = *ksz;
  }
  else /* get next key */
  {
    uint_t  val_size = *((uint_t*) key->data - 1U);
    uint_t  key_size = key->size;

    key->size = *((uchar_t*) key->data + key->size + val_size + sizeof (uint_t));
    key->data =  ((uchar_t*) key->data +  key_size + val_size + sizeof (uint_t) * 3U);

    if ( key->data >= (void*) block->free_ ) /* got all keys */
    {
      key->size = 0;
      key->data = NULL;
      if ( vsz ) *vsz = 0U;

      return NULL;
    }

    if ( vsz ) *vsz = *((uint_t*) key->data - 1U);
  } // end else

  return key;
}
/*  return non-malloc'ed value array */
sDBT      block_key_data  (IN sBlock *block, IN const sDBT *key)
{
  sDBT value = { 0 };

  if ( !key || (uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_ )
  { fprintf (stderr, "memory block key data%s", strmyerror (MYDB_ERR_FPARAM));
    return value;
  }

  value.size = *((uint_t*) (key->data) - 1U);
  value.data = ((uchar_t*) (key->data) + key->size);

  return value;
}
#ifdef  DEBUG
int       block_print_dbg (IN sBlock *block, IN const char *name)
{
  printf (">>> %s\n", name);
  printf ("type: %s\n", !((int*) block->memory_)[0] ? "Free" :
          ((int*) block->memory_)[0] == 1 ? "Pass" : "Leaf");
  printf ("nkvs: %d\t", ((int*) block->memory_)[1]);
  printf ("fset: %d\n", ((int*) block->memory_)[2]);
  printf ("free: %d\t", ((int*) block->memory_)[3]);
  printf ("pt_l: %d\n", ((int*) block->memory_)[4]);

  uint_t *imem = ((int*) block->memory_ + 5);
  for ( uint_t i = 0U; i < *block_nkvs (block); ++i )
  {
    uint_t ksz = *(imem);
    uint_t vsz = *(++imem); imem++;
    imem = (uint_t*) ((uchar_t*) imem + ksz + vsz);
    uint_t ptr = *(imem);
    printf ("ksz=%d vsz=%d k v ptr=%d\n", ksz, vsz, ptr);
    imem++;
  }
  printf ("\n");

  // for ( uint_t i = 4U; i < 5U * (*block_nkvs (block)) + 5U; ++i )
  // {
  //   printf (" %x", ((int*) block->memory_)[i]);
  //   if ( !((i - 3) % 5) )
  //     printf ("\n");
  // }
  // printf ("\n\n");
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
  long  offset = sizeof (sDBFH) + block->offset_ * block->size_;
  if ( mem )
    offset += block->owner_db_->head_.techb_count_ * block->size_;
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
eDBState  block_rotate_left (IN sBlock *block, IN sDBT *key)
{

  // get last

  // sDBT  iter = { 0 }, val = { 0 };
  // sDBT  *k = block_key_next (parent, &iter, NULL);
  // val.size = block_key_data (parent, &iter, &val);

  sDBT iter = { 0 }, *k_prev = NULL;
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (key, k) < 0 )
  {
    k = block_key_next (block, k, NULL);
    k_prev = k;
  }

  sDBT val = block_key_data (block, k_prev);



  return DONE;
}
eDBState  block_rotate_rght (IN sBlock *block, IN sBlock *lchild, IN sBlock *rchild, IN const sDBT *key)
{
  sDBT cur_k = { 0 }, value = { 0 };

  // get first
  if ( DONE != block_select (block, key, &cur_k, &value) )
    return FAIL;

  // sBlock *lchild = block_create (block, block_lptr (block, &cur_k));
  // sBlock *rchild = block_create (block, block_rptr (block, &cur_k));

  // block_change ();


  return DONE;
}

eDBState  block_rotate_left (IN sBlock *block, IN sDBT *key);
//-------------------------------------------------------------------------------------------------------------
/* !!! non-malloc'ed output */
/* ??? return k equal key or last k, smaller key */
eDBState  block_select (IN sBlock *block, IN const sDBT *key, OUT      sDBT *value, OUT sDBT *bkey)
{
  //---------------------------------------
  if ( *block_nkvs (block) )
  {
    sDBT iter = { 0 }, *pre_k = NULL;
    sDBT   *k = block_key_next (block, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    {
      pre_k = k;
      k = block_key_next (block, k, NULL);
    }
    //---------------------------------------
    if ( k || (k = pre_k) )
    {
      *bkey = *k;
      *value = block_key_data (block, k);
      return DONE;
    }
  }
  //---------------------------------------
  memset (bkey, 0, sizeof (*bkey));
  return FAIL;
}
eDBState  block_change (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, OUT sDBT *bkey)
{
  if ( (uchar_t*) bkey->data < block->data_ || (uchar_t*) bkey->data >= block->free_ )
  { fprintf (stderr, "memory block remove key%s", strmyerror (MYDB_ERR_FPARAM));
    return FAIL;
  }
  //-----------------------------------------
  uint_t need_free_size = (3U * sizeof (uint_t) + value->size + key->size);
  //-----------------------------------------
  if ( need_free_size > block->head_->free_size_ )
  { fprintf (stderr, "memory block remove key%s", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }
  //-----------------------------------------
  sDBT val = block_key_data (block, bkey);

  int  shifting_size = ((value->size - val.size) + (key->size - bkey->size));

  // ptr to next key-value
  uchar_t *lay_mem = ((uchar_t*) bkey->data + bkey->size + val.size);
  // move right or left
  memmove (lay_mem + shifting_size, lay_mem, block->free_ - lay_mem);

  *((uint_t*) bkey->data - 1U) = value->size;
  *((uint_t*) bkey->data - 2U) = key->size;

  bkey->size = key->size;
  memcpy (bkey->data,  key->data,  bkey->size);
  memcpy (((uchar_t*) bkey->data + bkey->size), value->data, value->size);
  //-----------------------------------------
  block->free_             += shifting_size;
  block->head_->free_size_ -= shifting_size;
  //-----------------------------------------
#ifdef  DEBUG
  block_print_dbg (block, "insert");
#endif
  return DONE;
}
eDBState  block_insert (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, OUT sDBT *bkey, uint_t Rptr)
{
  //-----------------------------------------
  uint_t need_free_size = (3U * sizeof (uint_t) + value->size + key->size);
  if ( need_free_size > block->head_->free_size_ )
  {
    fprintf (stderr, "memory block insert key%s", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }
  //-----------------------------------------
  // !!! memset (cur_k, 0, sizeof (*cur_k));
  
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  {
#ifdef  DEBUG
    // printf ("keynext: %d %x\n", k->size, k->data);
#endif
    k = block_key_next (block, k, NULL);
  }
#ifdef  DEBUG
  // if ( k ) printf ("keynext: %d %x\n", k->size, k->data);
#endif
  //-----------------------------------------
  uchar_t *lay_mem = NULL;
  if ( k && !key_compare (k, key) )
  {
    sDBT val = block_key_data (block, k);
    if ( val.size != value->size )
    {
      uint_t shifting = (value->size - val.size);

      // ptr to next key-value's
      lay_mem = ((uchar_t*) k->data + k->size + val.size);
      //-----------------------------------------
      // move right or left
      memmove (lay_mem + shifting, lay_mem, block->free_ - lay_mem);
      *((uint_t*) key->data - 1U) = value->size;

      //-----------------------------------------
      block->free_             += shifting;
      block->head_->free_size_ -= shifting;
      //-----------------------------------------
      lay_mem += shifting;
    }
    memcpy (val.data, value->data, value->size);

    /* KEY IS ALREADY CHANGED */
    if ( bkey ) *bkey = *k;

    // ??? Rptr
    if ( Rptr != MYDB_INVALIDPTR )
      *((uint_t*) lay_mem) = Rptr;
    else if ( *block_type (block) == Leaf )
      *((uint_t*) lay_mem) = 0;

    goto EXISTING;
  }
  else if ( k )
  {
    // key size to this key-value's
    lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * 2U);
    // move apart (раздвинуть)
    memmove (lay_mem + need_free_size, lay_mem, block->free_ - lay_mem);
  }
  else
  {
    lay_mem = block->free_;
  }
  //-----------------------------------------
  *(uint_t*) lay_mem =   key->size; lay_mem += sizeof (uint_t);
  *(uint_t*) lay_mem = value->size; lay_mem += sizeof (uint_t);
  //-----------------------------------------
  memmove (lay_mem,               key->data,   key->size);
  memmove (lay_mem + key->size, value->data, value->size);

  // Rptr
  lay_mem = ( lay_mem + key->size + value->size );
  if ( Rptr != MYDB_INVALIDPTR )
    *((uint_t*) lay_mem) = Rptr;
  else if ( *block_type (block) == Leaf )
    *((uint_t*) lay_mem) = 0;

  /* CHANGING KEY */
  if ( bkey )
  {
    bkey->size = key->size;
    bkey->data = (void*) lay_mem;
  }
  //-----------------------------------------
  block->free_             += need_free_size;
  block->head_->free_size_ -= need_free_size;
  //-----------------------------------------
  ++(*(block_nkvs (block)));
  //-----------------------------------------
EXISTING:;
#ifdef  DEBUG
  block_print_dbg (block, "insert");
#endif
  return DONE;
}
eDBState  block_delete (IN sBlock *block, IN const sDBT *key)
{
  //-----------------------------------------
  uint_t vsz = 0;
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, &vsz);
  while ( k && key_compare (k, key) < 0 )
  {
    k = block_key_next (block, k, NULL);
  }
  //-----------------------------------------
  if ( k && !key_compare (key, k) )
  {
    uint_t  size_mem = (k->size + vsz + sizeof (uint_t) * 3U);
    uchar_t *lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * 3U);
    memmove (lay_mem, lay_mem + size_mem, size_mem);

    block->free_             -= size_mem;
    block->head_->free_size_ += size_mem;
    //-----------------------------------------
    return DONE;
  }
  //-----------------------------------------
  return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
