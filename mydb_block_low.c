#include "stdafx.h"
#include "mydb_block_low.h"

//-------------------------------------------------------------------------------------------------------------
/*  function of iteration through the Block by keys
 *  to init the iteration - give a pointer to a key sDBT with zero size
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
#ifdef _DEBUG
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
    int    ptr =  *(int*) ((uchar_t*) imem + ksz + vsz);

    char str[100];
    memcpy (str, imem, ksz);
    str[ksz] = '\0';

    printf ("ksz=%d vsz=%d k='%s' v ptr=%d\n", ksz, vsz, str, ptr);
    imem = (uint_t*) ((uchar_t*) imem + ksz + vsz);
    imem++;
  }
  printf ("\n");
  return 0;
}
#endif
//-------------------------------------------------------------------------------------------------------------
/* LOW-LEVEL OPERATIONS */

eDBState  block_seek (IN sBlock *block)
{
  //-----------------------------------------
  long  offset = sizeof (sDBFH) + block->offset_ * block->size_;
  if ( block->is_mem_ )
    offset += block->owner_db_->head_.techb_count_ * block->size_;
  //-----------------------------------------
  /* lseek returns the offset, in bytes, of the new position from the beginning of the file */
  return (lseek (block->owner_db_->hfile_, offset, SEEK_SET) == offset) ? DONE : FAIL;
}
eDBState  block_read (IN sBlock *block)
{
  //-----------------------------------------
  if ( DONE != block_seek (block) )
    return FAIL;

  if ( read (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
    return FAIL;
  //-----------------------------------------
  block->dirty_ = false;
  return DONE;
}
eDBState  block_dump (IN sBlock *block)
{
#ifndef MYDB_NOCACHE
  if ( !block->dirty_ )
  { return DONE; }

  //-----------------------------------------
  /*  Грязный блок должен быть записан
   *  на диск перед вытеснением из кэша.
   */
  block->dirty_ = false;
#endif // !MYDB_NOCACHE
  //-----------------------------------------
  if ( DONE != block_seek (block) )
    return FAIL;

  if ( write (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
    return FAIL;
  //-----------------------------------------
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/* returns k, equal to or last smaller the key (non-malloc'ed output) */
eDBState  block_select (IN sBlock *block, IN const sDBT *key, OUT      sDBT *value,    OUT sDBT *bkey)
{
  //---------------------------------------
  if ( *block_nkvs (block) )
  {
    sDBT iter = { 0 }, pre_k = {0};
    sDBT   *k = block_key_next (block, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    {
      pre_k = *k;
      k = block_key_next (block, k, NULL);
    }
    //---------------------------------------
    if ( k || (k = &pre_k) )
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
/* change the bkey from given block to key and its value */
eDBState  block_change (IN sBlock *block, IN const sDBT *key, IN const sDBT *value, IN OUT sDBT *bkey, IN uint_t Rptr)
{
  if ( (uchar_t*) bkey->data < block->data_ || (uchar_t*) bkey->data >= block->free_ )
  { fprintf (stderr, "memory block change key%s", strmyerror (MYDB_ERR_FPARAM));
    return FAIL;
  }
  //-----------------------------------------
  sDBT val = block_key_data (block, bkey);
  int  shifting = ((value->size - val.size) + (key->size - bkey->size));

  /* ptr to next key-value */
  uchar_t *lay_mem = ((uchar_t*) bkey->data + bkey->size + val.size);
  /* move right (+) or left (-) */
  memmove (lay_mem + shifting, lay_mem, block->free_ - lay_mem);

  *((uint_t*) bkey->data - 1U) = value->size;
  *((uint_t*) bkey->data - 2U) =   key->size;

  bkey->size = key->size;
  memcpy (bkey->data,  key->data,  bkey->size);
  memcpy (((uchar_t*) bkey->data + bkey->size), value->data, value->size);
  //-----------------------------------------
  block->free_             += shifting;
  block->head_->free_size_ -= shifting;
  //-----------------------------------------
  /* Rptr for key */
  lay_mem = (lay_mem + key->size + value->size);
  if ( Rptr != MYDB_INVALIDPTR )
  { *((uint_t*) lay_mem) = Rptr; }
  else
  { *((uint_t*) lay_mem) = MYDB_INVALIDPTR; }
  //-----------------------------------------

#ifdef _DEBUG
  block_print_dbg (block, "change");
#endif

  // block->dirty_ = true;
  return DONE;
}
eDBState  block_insert (IN sBlock *block, IN const sDBT *key, IN const sDBT *value,    OUT sDBT *bkey, IN uint_t Rptr)
{
  //-----------------------------------------
  uint_t need_free_size = (3U * sizeof (uint_t) + value->size + key->size);
  if ( need_free_size > block->head_->free_size_ )
  {
    fprintf (stderr, "memory block insert key%s", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  { k = block_key_next (block, k, NULL); }
  //-----------------------------------------
  uchar_t *lay_mem = NULL;
  if ( k && !key_compare (k, key) )
  {
#ifdef  _DEBUG
    char str[100];

    memset (str, 0, 100);
    memcpy (str, key->data, key->size);
    printf ("'%s'=", str);

    memset (str, 0, 100);
    memcpy (str, k->data, k->size);
    printf ("'%s'\n", str);
#endif // _DEBUG

    /* if there is the equal key */
    sDBT val = block_key_data (block, k);

    /* RPtr for k */
    lay_mem = ((uchar_t*) k->data + k->size + val.size);

    if ( val.size != value->size )
    {
      int  shifting = (value->size - val.size);
      //-----------------------------------------
      /* move right (+) or left (-) */
      memmove (lay_mem + shifting, lay_mem, block->free_ - lay_mem);

      *((uint_t*) k->data - 1U) = val.size = value->size;
      //-----------------------------------------
      block->free_ += shifting;
      block->head_->free_size_ -= shifting;
      //-----------------------------------------
      /* RPtr for k */
      lay_mem = ((uchar_t*) k->data + k->size + val.size);
    }
    memcpy (val.data, value->data, value->size);

    /* key is exactly the same */
    if ( bkey ) { *bkey = *k; }

    /* Rptr for k */
    if ( Rptr != MYDB_INVALIDPTR )
    { *((uint_t*) lay_mem) = Rptr; }
    else
    { *((uint_t*) lay_mem) = MYDB_INVALIDPTR; }
  } 
  else
  {
    if ( k )
    {
      /* size for k */
      lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * 2U);
      /* move apart (раздвинуть) */
      memmove (lay_mem + need_free_size, lay_mem, block->free_ - lay_mem);
    }
    else
    {
      /* insert to end */
      lay_mem = block->free_;
    }
    //-----------------------------------------
    *(uint_t*) lay_mem =   key->size; lay_mem += sizeof (uint_t);
    *(uint_t*) lay_mem = value->size; lay_mem += sizeof (uint_t);
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
    lay_mem = (lay_mem + key->size + value->size);
    if ( Rptr != MYDB_INVALIDPTR )
    { *((uint_t*) lay_mem) = Rptr; }
    else
    { *((uint_t*) lay_mem) = MYDB_INVALIDPTR; }
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

  //-----------------------------------------
  // block->dirty_ = true;
  return DONE;
}
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
    uchar_t *lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * ((lptr) ? 3U : 2U));
    uint_t  size_mem = (k->size + vsz + sizeof (uint_t) * 3U);
    
    lay_mem += size_mem;
    /* move together (сдвинуть) */
    memmove (lay_mem - size_mem, lay_mem, block->free_ - lay_mem);

    memset (block->free_ - size_mem, 0, size_mem);

    block->free_             -= size_mem;
    block->head_->free_size_ += size_mem;
    //-----------------------------------------
#ifdef _DEBUG
    block_print_dbg (block, "delete");
#endif

    // block->dirty_ = true;
    return DONE;
  }
  //-----------------------------------------
  return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
