#include "stdafx.h"
#include "mydb_block.h"
#include "mydb_techb.h"

//-------------------------------------------------------------------------------------------------------------
/* DECORATIVE OPERATIONS */

eDBNT*    block_type (IN sBlock *block)
{ return  &block->head_->node_type_; }
uint_t*   block_nkvs (IN sBlock *block)
{ return  &block->head_->kvs_count_; }
uint_t*   block_lptr (IN sBlock *block, IN const sDBT *key)
{
  /* | ptr | ksz | vsz | key | val | */
  return (key) ? ((uint_t*) key->data - 3U) : ((uint_t*) block->free_ - 1U);
}
uint_t*   block_rptr (IN sBlock *block, IN const sDBT *key)
{
  /* | ksz | vsz | key | val | ptr | */
  return (key) ? (uint_t*) ((uchar_t*) key->data + key->size + *((uint_t*) key->data - 1U)) :
    &block->head_->pointer_0_;
}
//-------------------------------------------------------------------------------------------------------------
bool      block_isfull (IN const sBlock *block)
{ return ( 2U * block->head_->free_size_ <= (block->size_ - sizeof (sDBHB)) ); }
bool      block_enough (IN const sBlock *block)
{ return ( 4U * block->head_->free_size_ >= (block->size_ - sizeof (sDBHB)) ); }
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
    if( vsz ) *vsz = *(ksz + 1U);
  
    key->data = (void*) (ksz + 2U);
    key->size = *ksz;
    return key; 
  } // !!!
  else /* get next key */
  {
    uint_t  ptr_i = 0;
    uint_t  val_size = *((uint_t*) ((uchar_t*) key->data - sizeof (uint_t)));

    key->size = *((uchar_t*) key->data + key->size + val_size + sizeof (ptr_i));
    key->data =  ((uchar_t*) key->data + key->size + val_size + sizeof (ptr_i)
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
eDBState  block_insert (IN sBlock *block, IN const sDBT *key, IN  const sDBT *value)
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
  while ( k && key < k )
  {
    k = block_key_next (block, k, NULL);
  }
  //-----------------------------------------
  if ( k )
  {
    lay_mem = ((uchar_t*) k->data - sizeof (uint_t) * 3);
    memmove (lay_mem + need_free_size, lay_mem, block->free_ - lay_mem);

    ( *(uint_t*) lay_mem) = 0U; /* ptr */
    ++((uint_t*) lay_mem);
  }
  else
  {
    lay_mem = block->data_;
    need_free_size -= sizeof (uint_t); /* ptr */
  }
  //-----------------------------------------
  *(uint_t*) lay_mem =   key->size;  ++((uint_t*) lay_mem);
  *(uint_t*) lay_mem = value->size;  ++((uint_t*) lay_mem);
  //-----------------------------------------
  memmove (lay_mem,   key->data,   key->size); lay_mem += key->size;
  memmove (lay_mem, value->data, value->size); lay_mem += value->size;
  //-----------------------------------------
  block->free_ += need_free_size;
  block->head_->free_size_ -= need_free_size;
  //-----------------------------------------
  ++(*(block_nkvs (block)));
  //-----------------------------------------
#define _DEBUG
#ifdef  _DEBUG
  for ( int i = 0; i < *block_nkvs (block) * 5 + 5; ++i )
    printf (" %x", ((int*) block->memory_)[i]);
  printf ("\n\n");
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
/* HIGH-LEVEL OPERATIONS */

eDBState  block_select_data (IN sBlock *block,  IN const sDBT *key, OUT      sDBT *value)
{
  const char *error_prefix = "memory block searching";
  eDBState    result = DONE;
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  {
    k = block_key_next (block, k, NULL);
  }
  //-----------------------------------------
  if ( k && !key_compare (key, k) )
  {
    void   *val = NULL;
    uint_t  vsz = block_key_data (block, k, &val);
    //-----------------------------------------
    value->data = malloc (sizeof (uchar_t) * vsz);
    if ( !value->data )
    {
      fprintf (stderr, "%s%s", error_prefix, strerror (errno));
      result = FAIL;
    }
    else
    {
      value->size = vsz;
      memcpy (value->data, val, vsz);
    }
    //-----------------------------------------
  }
  else if ( (*block_type (block)) == Leaf )
  {
    result = FAIL;
    value->data = NULL;
    value->size = 0;
  }
  else
  {
    sBlock *child = block_create (block->owner_db_, (*block_lptr (block, k)));
    if ( child )
    {
      result = block_select_data (child, key, value);
      block_destroy (child);
    }
    else result = FAIL;
  }
  //-----------------------------------------
  return result;
}
eDBState  block_add_nonfull (IN sBlock *block,  IN const sDBT *key, IN const sDBT *value)
{
  const char *error_prefix = "memory block add non-full";
  eDBState  result = DONE;
  sBlock   *ychild = NULL, *zchild = NULL;
  //-----------------------------------------
  if ( (*block_type (block)) == Leaf )
  {
    if ( DONE != block_insert (block, key, value) )
    {
      result = FAIL;
      goto NONFULL_FREE;
    }
    if ( DONE != block_write  (block, true) )
    {
      result = FAIL;
      fprintf (stderr, "%s%s Offset is %d.\n", error_prefix,
               strmyerror (MYDB_ERR_BWRITE), block->offset_);
    }
  }
  else
  { //-----------------------------------------
    sDBT iter = { 0 };
    sDBT   *k = block_key_next (block, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    {
      k = block_key_next (block, k, NULL);
    }
    //-----------------------------------------
    ychild = block_create (block->owner_db_, *block_lptr (block, k));
    if ( !ychild )
    {
      result = FAIL;
      goto NONFULL_FREE;
    }
    //-----------------------------------------
    if ( block_isfull (ychild) )
    {
      zchild = block_create (block->owner_db_, 0U);
      if ( !zchild )
      { result = FAIL;
        fprintf (stderr, "%s%s", error_prefix, strerror (errno));
        goto NONFULL_FREE;
      }
      //-----------------------------------------
      if ( DONE != block_split_child (block, ychild, zchild) )
      {
        result = FAIL;
        goto NONFULL_FREE;
      }
    }
    else  zchild = ychild;
    //-----------------------------------------
    if ( DONE != block_add_nonfull (zchild, key, value) )
    {
      result = FAIL;
      goto NONFULL_FREE;
    }
    if ( DONE != block_write       (zchild, true) )
    {
      result = FAIL;
      fprintf (stderr, "%s%s Offset is %d.\n", error_prefix,
               strmyerror (MYDB_ERR_BWRITE), zchild->offset_);
      // goto NONFULL_FREE;
    } 
  } // end else
  //-----------------------------------------
NONFULL_FREE:;
  block_destroy (ychild); ychild = NULL;
  block_destroy (zchild); zchild = NULL;
  //-----------------------------------------
  return result;
}
eDBState  block_deep_delete (IN sBlock *block,  IN const sDBT *key)
{
  const char *error_prefix = "memory block deep delete";
  eDBState    result = DONE;
  sDBT         value = { 0 };
  sBlock     *ychild = NULL, *zchild = NULL;
  //-----------------------------------------
  /* if key is in current block */
  if ( DONE == block_select_data (block, key, &value) ) // <-- malloc'ed
  {
    if ( (*block_type (block)) == Leaf )
    {
      result = block_delete (block, key);
    }
    else /* block is Pass */
    {
      //-----------------------------------------
      ychild = block_create (block, *block_lptr (block, key));
      if ( !ychild )
      {
        result = FAIL;
        goto DDEL_FREE;
      }
      /*  If it goes through, and it is a block
       *  with non-enough to deleting elements.
       */
      if ( block_enough (ychild) ) /* y.nkvs >= t */
      {
        /*-----------------------------------------
         *  находим k - предшественника key в поддереве,
         *  корнем которого является y. Рекурсивно удаляем
         *  k и заменяем key в x ключом k.
         *  (поиск k за один проход вниз)
         *
         *   x: ..key..      x: ..(key<-k)..
         *       /   \   =>       /   \
         *      y     z       k<-y     z
         *------------------------------------------*/
        block_recursively_delete_key_in_left_branch (ychild, key);
      }
      else /* z has NOT enough */
      {
        /*-----------------------------------------
         *  симметрично обращаемся к дочернему по отношению
         *  к x узлу z, который слудует за ключом key в узле x.
         *     x: ..key...
         *         /   \
         *        z     y
         *------------------------------------------*/
        zchild = block_create (block, block_rptr (block, key));
        if ( block_enough (zchild) ) /* z.nkvs >= t */
        {
          /*   находим k - следующий за key ключ в поддереве,
           *   корнем которого является z.Рекурсивно удаляем
           *   k и заменяем key в x ключом k.
           *   (поиск k за один проход вниз)
           */
          block_recursively_delete_key_in_rght_branch (zchild, key);
        }
        else /* block_NOT_enough_in (z or y) */
        {
          block_merge_child (block, ychild, zchild);
        } // end else both not enough
      } // end else z enough
    } // end else block not Leaf
  } // end if key in x
  else /* key not in x */
  {
    uint_t ptr = 0;
    sBlock *ychild = block_create (block, ptr); // that key in y or in child (y), ...
    if ( !ychild )
    {
      result = FAIL;
      goto DDEL_FREE;
    }

    if ( !block_enough (ychild) ) /* y.nkvs < t */
    {
      if ( 1 ) // one y's neighbour.nkvs >= t
      {
        /*------------------------------------------------------------------------
         *   x: ..k..
         *       / \    =>   x->kv->y, z->k'v'->x
         *      y   z
         *------------------------------------------------------------------------*/
      }
      else
      {
        /*------------------------------------------------------------------------
         *   x : ..k..      x : .....
         *        / \    =>      /
         *       y   z         ykz
         *------------------------------------------------------------------------*/
        // del child (x, ? )[key]
      }
    }
  }
  //------------------------------------------------------------------------
DDEL_FREE:;
  //------------------------------------------------------------------------
  block_destroy (ychild); ychild = NULL;
  block_destroy (zchild); zchild = NULL;
  //------------------------------------------------------------------------
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/* ychild - full node; zchild - new free node */
eDBState  block_split_child (IN sBlock *parent, IN  sBlock *ychild, OUT sBlock *zchild)
{
  eDBState result = DONE;
  /*-----------------------------------------
   *    x - parent,   y,z - childs,   k - key
   *    x: .....      x: ..k..
   *        /     =>      / \
   *      ykz            y   z
   *-----------------------------------------*/
  (*block_type (zchild)) = (*block_type (zchild));
  //-----------------------------------------
  /* Claculate the number of items to deligate */
  uint_t amount_size = 0U, count = 0U, val_size;

  sDBT iter = { 0 };
  sDBT   *k = block_key_next (ychild, &iter, &val_size);
  while ( k && 4U * amount_size < ychild->head_->free_size_ )
  { /* Iterate through the element, those stay in ychild */
    ++count;
    amount_size += (k->size + val_size) + (3U * sizeof (uint_t));
    k = block_key_next (ychild, k, &val_size);
  }
  /*  I hope, k cannot be equal to NULL, means,
   *  the whole block must be copied to another
   *-----------------------------------------*/
  sDBT val = { 0 };
  val.size = block_key_data (ychild, k, &val.data);
  /* move the middle element into the parent block */
  if ( DONE != block_insert (parent, k, &val) )
  {
    result = FAIL;
    goto SPLIT_FREE;
  }
  (*block_rptr (parent, k)) = zchild->offset_;

  ++count;
  amount_size += (k->size + val_size) + (3U * sizeof (uint_t));
  //-----------------------------------------
  uint_t bytes_count = ((ychild->free_ - ychild->data_) - amount_size);
  memcpy (zchild->data_, ychild->data_ + amount_size, bytes_count);
  (*block_nkvs (zchild)) = (*block_nkvs (ychild) - count); /* latest elements of ychild */
  (*block_nkvs (ychild)) = (count - 1U);  /* remaining elements without the middle one */
  /* ptr's have already copied with elements data */
  //-----------------------------------------
  if ( DONE != block_write (ychild, true)
    || DONE != block_write (zchild, true)
    || DONE != block_write (parent, true) )
  { result = FAIL;
    // goto end;
  }
  //-----------------------------------------*/
SPLIT_FREE:;
  //-----------------------------------------
  return result;
}
eDBState  block_merge_child (IN sBlock *parent, OUT sBlock *ychild, IN  sBlock *zchild)
{ /*-----------------------------------------
   *  x: ..k..      x: .....
   *      / \   =>       /
   *     y   z         ykz
   *-----------------------------------------*/

   // block_insert (child, key, value); // ???
   // block_delete (block, k /* ??? */);
   // ? del x.ptr to z
   // ? z.free

   // + рекурсивно удаляем key из y
   // ? block_delete_ (child, key);

  uint_t   ptr_size = sizeof (zchild->head_->pointer_0_);
  uint_t  need_size = ((zchild->free_ - zchild->data_) + ptr_size);
  if ( ychild->head_->free_size_ < need_size )
    return FAIL;
  //-----------------------------------------
  memset (ychild->free_, zchild->data_ - ptr_size, need_size);
  //-----------------------------------------
  ychild->free_ += need_size;
  ychild->head_->free_size_ -= need_size;
  //-----------------------------------------
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/*
  if key in x:
     if x is leaf:
        del x[key]
     else:
        y = child(x, key)
        if y.nkvs >= t:
           try:
              while k in y and k++ < key:
                 if y != leaf:
                    y = child(y, k)
                 else:
                    del y[k]
                    raise found
          except:
             pass
          x[key] = x[k]
           
           находим k - предшественника key в поддереве,
           корнем которого является y. Рекурсивно удаляем
           k и заменяем key в x ключом k.
           (поиск k за один проход вниз)
        else:
           z = child(x, key+1)
           
           симметрично обращаемся к дочернему по отношению
           к x узлу z, который слудует за ключом key в узле x.
           if z.nkvs >= t:
               try:
                  while k in z and ++k < key:
                     if z != leaf:
                        z = child(z, k)
                     else:
                        del z[k]
                        raise found
               except:
                  pass
               x[key] = x[k]

               находим k - следующий за key ключ в поддереве,
               корнем которого является z. Рекурсивно удаляем
               k и заменяем key в x ключом k.
               (поиск k за один проход вниз)
           else:
               y.append(key)
               y.append([k for k in z])
               del x[k]
               del x.ptr to z
               z.free
               + рекурсивно удаляем key из y
  else: # key not in x
       y = child(x, i) that key in y or in child(y), ...
       if y.nkvs < t:
          if one y's neighbour.nkvs >= t
             x: ..k..
                 / \    =>   x->kv->y, z->k'v'->x
                y   z        
          else:
             x: ..k..         x:.....
                 / \    =>       /
                y   z          ykz
       del child(x, ?)[key]
  //------------------------------------------------------------------------
  */
//-------------------------------------------------------------------------------------------------------------
uint_t    block_offset2free (IN sDB *db)
{
  const char *error_prefix = "";

  uint_t offset = techb_get_index_of_first_free_bit (db);
  db->head_.nodes_count_ += 1;

  if ( DONE != techb_set_bit (db, offset, true) )
  {
    // mydb_errno =
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    offset = db->head_.block_count_; // return ??
  }

  return offset;
}
//-------------------------------------------------------------------------------------------------------------
sBlock*   block_create  (IN sDB    *db, IN uint_t offset)
{
  const char *error_prefix = "memory block creation";
  bool fail = false;
  sBlock *block = calloc (1U, sizeof (sBlock));
  //-----------------------------------------
  if ( block )
  {
    block->owner_db_ = db;
    block->size_ = db->head_.page_size_;

    block->memory_ = calloc (block->size_, sizeof (uchar_t));
    if ( !block->memory_ )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto BLOCK_FREE;
    }

    block->head_ = (sDBHB*) block->memory_;
    if ( offset && offset < db->head_.block_count_ )
    {
      block->head_->db_offset_ = offset;
      if ( DONE != block_read (block) )
      {
        fail = true;
        mydb_errno = MYDB_ERR_FPARAM;
        fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
        goto BLOCK_FREE;
      }
    }
    else if ( offset )
    {
      fail = true;
      mydb_errno = MYDB_ERR_FPARAM;
      fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
      goto BLOCK_FREE;
    }
    else
    {
      block->head_->db_offset_ = block_offset2free (db);
      if ( block->head_->db_offset_ >= db->head_.block_count_ )
      {
        fail = true;
        mydb_errno = MYDB_ERR_NFREEB;
        fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
        goto BLOCK_FREE;
      }

      block->head_->free_size_ = block->size_ - sizeof (sDBFH);
      block->head_->kvs_count_ = 0U;
      block->head_->node_type_ = Free;
      block->head_->pointer_0_ = 0U;
    }

    block->offset_ =  block->head_->db_offset_;
    block->data_   = (block->memory_ + sizeof (sDBHB));
    block->free_   = (block->memory_ + (block->size_ - block->head_->free_size_));
  }
  else
  {
    fail = true;
    mydb_errno = MYDB_ERR_FPARAM;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    // goto BLOCK_FREE;
  }
  //-----------------------------------------
BLOCK_FREE:;
  if ( fail )
  {
    block_destroy (block);
    block = NULL;
  }
  //-----------------------------------------
  return block;
}
void      block_destroy (IN sBlock *block)
{
  //-----------------------------------------
  if (block )
    free (block->memory_);
  free (block);  
}
//-------------------------------------------------------------------------------------------------------------
