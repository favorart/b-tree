#include "stdafx.h"
#include "mydb_block_low.h"

//-------------------------------------------------------------------------------------------------------------
/*  Entry layout inside a block (all numbers are uint32, unaligned):
 *     | ksz | vsz | key | val | rptr |
 *  key->data points to the key bytes, so ksz/vsz are right before it.
 */
#define  VSZ_OF(kdata)  mydb_rd32 ((uchar_t*) (kdata) - 1U * sizeof (uint32_t))
//-------------------------------------------------------------------------------------------------------------
/*  function of iteration through the Block by keys
 *  to init the iteration - give a pointer to a key sDBT with zero size
 *  to stop the iteration - function returns NULL
 */
sDBT*     block_key_next  (IN sBlock *block, IN OUT   sDBT *key, OUT uint_t *vsz)
{
  if ( !key || (key->size && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_)) )
  { fprintf (stderr, "memory block key next%s\n", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }

  if ( *block_nkvs (block) == 0U )
    return NULL;

  uchar_t *entry;
  if ( !key->size ) /* get first key */
    entry = block->data_;
  else /* get next key: skip key, value and the right pointer */
    entry = (uchar_t*) key->data + key->size + VSZ_OF (key->data) + sizeof (uint32_t);

  if ( entry >= block->free_ ) /* got all keys */
  {
    key->size = 0;
    key->data = NULL;
    if ( vsz ) *vsz = 0U;
    return NULL;
  }

  key->size = mydb_rd32 (entry);
  key->data = entry + 2U * sizeof (uint32_t);
  if ( vsz ) *vsz = VSZ_OF (key->data);

  return key;
}
/*  return non-malloc'ed value array */
sDBT      block_key_data  (IN sBlock *block, IN const sDBT *key)
{
  sDBT value = { 0 };

  if ( !key || (uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_ )
  { fprintf (stderr, "memory block key data%s\n", strmyerror (MYDB_ERR_FPARAM));
    return value;
  }

  value.size = VSZ_OF (key->data);
  value.data = ((uchar_t*) (key->data) + key->size);

  return value;
}
#ifdef  _DEBUG
int       block_print_dbg (IN sBlock *block, IN const char *name)
{
  sDBHB *h = block->head_;
  printf (">>> %s\n", name);
  printf ("type: %s\t", (h->node_type_ == Free) ? "Free" :
                        (h->node_type_ == Pass) ? "Pass" : "Leaf");
  printf ("nkvs: %u\t", (unsigned) h->kvs_count_);
  printf ("offs: %u\t", (unsigned) h->db_offset_);
  printf ("free: %u\t", (unsigned) h->free_size_);
  printf ("ptr0: %d\n", (int) h->pointer_0_);

  uint_t vsz = 0U;
  sDBT iter = { 0 };
  for ( sDBT *k = block_key_next (block, &iter, &vsz); k; k = block_key_next (block, k, &vsz) )
    printf ("ksz=%u vsz=%u k='%.*s' rptr=%d\n", (unsigned) k->size, (unsigned) vsz,
            (int) k->size, (const char*) k->data, (int) block_rptr (block, k));
  printf ("\n");
  return 0;
}
#endif // _DEBUG
//-------------------------------------------------------------------------------------------------------------
/* LOW-LEVEL OPERATIONS */

eDBState  block_seek (IN sBlock *block)
{
  //-----------------------------------------
  /* | file header | bitmap pages (techb_count_) | data pages (block_count_) | */
  off_t  offset = (off_t) sizeof (sDBFH) + (off_t) block->offset_ * block->size_;
  if ( block->is_mem_ )
    offset += (off_t) block->owner_db_->head_.techb_count_ * block->size_;
  //-----------------------------------------
  /* lseek returns the offset, in bytes, of the new position from the beginning of the file */
  return (lseek (block->owner_db_->hfile_, offset, SEEK_SET) == offset) ? DONE : FAIL;
}
eDBState  block_read (IN sBlock *block)
{
  //-----------------------------------------
  if ( DONE != block_seek (block) )
    return FAIL;

  if ( read (block->owner_db_->hfile_, block->memory_, block->size_) != (ssize_t) block->size_ )
    return FAIL;
  //-----------------------------------------
  block->dirty_ = false;
  return DONE;
}
eDBState  block_dump (IN sBlock *block)
{
  if ( !block->dirty_ )
  { return DONE; }
  //-----------------------------------------
  if ( DONE != block_seek (block) )
    return FAIL;

  if ( write (block->owner_db_->hfile_, block->memory_, block->size_) != (ssize_t) block->size_ )
    return FAIL;
  //-----------------------------------------
  /*  Грязный блок должен быть записан
   *  на диск перед вытеснением из кэша.
   */
  block->dirty_ = false;
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/*  Replace the entry bkey (a key living in this block) by key:value in place.
 *  The right pointer of the entry is kept, unless Rptr != MYDB_INVALIDPTR.
 *  On success bkey points to the new key.
 */
eDBState  block_change (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, IN OUT sDBT *bkey, IN uint_t Rptr)
{
  if ( (uchar_t*) bkey->data < block->data_ || (uchar_t*) bkey->data >= block->free_ )
  { fprintf (stderr, "memory block change key%s\n", strmyerror (MYDB_ERR_FPARAM));
    return FAIL;
  }
  //-----------------------------------------
  sDBT val = block_key_data (block, bkey);
  long shifting = ((long) value->size - (long) val.size) + ((long) key->size - (long) bkey->size);
  if ( shifting > 0 && (uint32_t) shifting > block->head_->free_size_ )
  { fprintf (stderr, "memory block change key%s\n", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }

  /* ptr to the right pointer of the entry, the rest of the block follows it */
  uchar_t *lay_mem = ((uchar_t*) bkey->data + bkey->size + val.size);
  /* move right (+) or left (-) */
  memmove (lay_mem + shifting, lay_mem, block->free_ - lay_mem);

  mydb_wr32 ((uchar_t*) bkey->data - 1U * sizeof (uint32_t), (uint32_t) value->size);
  mydb_wr32 ((uchar_t*) bkey->data - 2U * sizeof (uint32_t), (uint32_t)   key->size);

  bkey->size = key->size;
  memmove (bkey->data, key->data, bkey->size);
  memmove (((uchar_t*) bkey->data + bkey->size), value->data, value->size);
  //-----------------------------------------
  block->free_             += shifting;
  block->head_->free_size_ -= (uint32_t) shifting;
  if ( shifting < 0 )
    memset (block->free_, 0, (size_t) -shifting);
  //-----------------------------------------
  /* Rptr for key */
  if ( Rptr != MYDB_INVALIDPTR )
  { mydb_wr32 ((uchar_t*) bkey->data + key->size + value->size, Rptr); }
  //-----------------------------------------
#ifdef _DEBUG
  block_print_dbg (block, "change");
#endif
  return DONE;
}
eDBState  block_insert (IN sBlock *block, IN const sDBT *key, IN const sDBT *value,    OUT sDBT *bkey, IN uint_t Rptr)
{
  //-----------------------------------------
  uint_t need_free_size = (uint_t) (MYDB_ENTRY_OVERHEAD + value->size + key->size);
  if ( need_free_size > block->head_->free_size_ )
  {
    fprintf (stderr, "memory block insert key%s\n", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  { k = block_key_next (block, k, NULL); }
  //-----------------------------------------
  if ( k && !key_compare (k, key) )
  {
    /* if there is the equal key - replace its value */
    if ( DONE != block_change (block, key, value, k, Rptr) )
      return FAIL;
    if ( bkey ) { *bkey = *k; }
  }
  else
  {
    uchar_t *lay_mem = NULL;
    if ( k )
    {
      /* size for k */
      lay_mem = ((uchar_t*) k->data - sizeof (uint32_t) * 2U);
      /* move apart (раздвинуть) */
      memmove (lay_mem + need_free_size, lay_mem, block->free_ - lay_mem);
    }
    else
    {
      /* insert to end */
      lay_mem = block->free_;
    }
    //-----------------------------------------
    mydb_wr32 (lay_mem, (uint32_t)   key->size); lay_mem += sizeof (uint32_t);
    mydb_wr32 (lay_mem, (uint32_t) value->size); lay_mem += sizeof (uint32_t);
    //-----------------------------------------
    memmove (lay_mem,               key->data,   key->size);
    memmove (lay_mem + key->size, value->data, value->size);

    if ( bkey )
    {
      /* changing out key */
      bkey->size = key->size;
      bkey->data = (void*) lay_mem;
    }

    /* Rptr for key */
    mydb_wr32 (lay_mem + key->size + value->size, Rptr);
    //-----------------------------------------
    block->free_ += need_free_size;
    block->head_->free_size_ -= need_free_size;
    //-----------------------------------------
    ++(*(block_nkvs (block)));
  }
  //-----------------------------------------
#ifdef _DEBUG
  block_print_dbg (block, "insert");
#endif
  return DONE;
}
/*  Remove the entry with the given key together with its
 *  left (lptr == true) or right (lptr == false) child pointer.
 */
eDBState  block_delete (IN sBlock *block, IN const sDBT *key, IN bool lptr)
{
  uint_t vsz = 0;
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, &vsz);
  while ( k && key_compare (k, key) < 0 )
  { k = block_key_next (block, k, &vsz); }
  //-----------------------------------------
  if ( k && !key_compare (key, k) )
  {
    /* also delete the LPtr/Rptr for k */
    uchar_t *lay_mem = ((uchar_t*) k->data - sizeof (uint32_t) * ((lptr) ? 3U : 2U));
    uint_t  size_mem = (uint_t) (k->size + vsz + MYDB_ENTRY_OVERHEAD);

    lay_mem += size_mem;
    /* move together (сдвинуть) */
    memmove (lay_mem - size_mem, lay_mem, block->free_ - lay_mem);

    memset (block->free_ - size_mem, 0, size_mem);

    block->free_             -= size_mem;
    block->head_->free_size_ += size_mem;
    --(*(block_nkvs (block)));
    //-----------------------------------------
#ifdef _DEBUG
    block_print_dbg (block, "delete");
#endif
    return DONE;
  }
  //-----------------------------------------
  return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
