#include "stdafx.h"
#include "mydb_block_low.h"
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
#ifdef DEBUG
  if ( key && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_) )
  { fprintf (stderr, "memory block left ptr%s", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }
#endif // DEBUG
  /* | ptr | ksz | vsz | key | val | */
  return (key) ? ((uint_t*) key->data - 3U) : ((uint_t*) block->free_ - 1U);
}
uint_t*   block_rptr (IN sBlock *block, IN const sDBT *key)
{
#ifdef DEBUG
  if ( key && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_) )
  { fprintf (stderr, "memory block right ptr%s", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }
#endif // DEBUG
  /* | ksz | vsz | key | val | ptr | */
  return (key) ? (uint_t*) ((uchar_t*) key->data + key->size + *((uint_t*) key->data - 1U)) :
    &block->head_->pointer_0_;
}
//-------------------------------------------------------------------------------------------------------------
bool      block_isfull (IN const sBlock *block)
{ return ( 3U * block->head_->free_size_ <= 1U * (block->size_ - sizeof (sDBHB)) ); }
bool      block_enough (IN const sBlock *block)
{ return ( 4U * block->head_->free_size_ <= 3U * (block->size_ - sizeof (sDBHB)) ); }
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
    sDBT val = block_key_data (block, k);
    //-----------------------------------------
    value->data = malloc (sizeof (uchar_t) * val.size);
    if ( !value->data )
    {
      fprintf (stderr, "%s%s", error_prefix, strerror (errno));
      result = FAIL;
    }
    else
    {
      value->size = val.size;
      memcpy (value->data, val.data, val.size);
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
    sBlock *child = block_create (block->owner_db_, *block_lptr (block, k));
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
    if ( DONE != block_insert (block, key, value, NULL, MYDB_INVALIDPTR) )
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
      zchild = block_create (block->owner_db_, MYDB_OFFSET2NEW);
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
  block_destroy (ychild);
  if( zchild != ychild )
    block_destroy (zchild);

  ychild = NULL;
  zchild = NULL;
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
      ychild = block_create (block->owner_db_, *block_lptr (block, key));
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
        block_recursively_delete_key_in_left_branch (block, ychild, key);
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
        zchild = block_create (block->owner_db_, *block_rptr (block, key));
        if ( block_enough (zchild) ) /* z.nkvs >= t */
        {
          /*   находим k - следующий за key ключ в поддереве,
           *   корнем которого является z.Рекурсивно удаляем
           *   k и заменяем key в x ключом k.
           *   (поиск k за один проход вниз)
           */
          block_recursively_delete_key_in_rght_branch (block, zchild, key);
        }
        else /* block_NOT_enough_in (z or y) */
        {
          block_merge_child (block, ychild, zchild, key);
        } // end else both not enough
      } // end else z enough
    } // end else block not Leaf
  } // end if key in x
  else /* key not in x */
  {
    uint_t ptr = MYDB_OFFSET2NEW;
    ychild = block_create (block->owner_db_, ptr); // that key in y or in child (y), ...
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
  (*block_type (zchild)) = (*block_type (ychild));
  //-----------------------------------------
  /* Claculate the number of items to deligate */
  uint_t amount_size = 0U, count = 0U, vsz = 0U;

  sDBT iter = { 0 };
  sDBT   *k = block_key_next (ychild, &iter, &vsz);
  while ( k && amount_size < (ychild->free_ - ychild->data_) / 2U )
  { /* Iterate through the elements, those stay in ychild */
    ++count;
    amount_size += (k->size + vsz) + (3U * sizeof (uint_t));
    k = block_key_next (ychild, k, &vsz);
  }
  /*  I hope, k cannot be equal to NULL, means,
   *  the whole block must be copied to another
   *-----------------------------------------*/
  sDBT val = block_key_data (ychild, k);
  /* move the middle element into the parent block */
  if ( DONE != block_insert (parent, k, &val, NULL, zchild->offset_) )
  {
    result = FAIL;
    goto SPLIT_FREE;
  }

  ++count;
               amount_size  += (k->size + val.size) + (3U * sizeof (uint_t));
  ychild->head_->free_size_ += (k->size + val.size) + (3U * sizeof (uint_t));
  //-----------------------------------------
  uint_t bytes_count = ( (ychild->free_ - ychild->data_) - amount_size );
  memcpy (zchild->data_, ychild->data_ + amount_size, bytes_count);

  (*block_nkvs (zchild)) = (*block_nkvs (ychild) - count); /* latest elements of ychild */
  (*block_nkvs (ychild)) = (count - 1U);  /* remaining elements without the middle one */
  //-----------------------------------------
  /* ptr's have already copied with elements data */
  //-----------------------------------------
  ychild->head_->free_size_ += bytes_count;
  ychild->free_ = ychild->memory_ + (ychild->size_ - ychild->head_->free_size_);

  zchild->head_->free_size_ -= bytes_count;
  zchild->free_ = zchild->memory_ + (zchild->size_ - zchild->head_->free_size_);
  //-----------------------------------------
  if ( DONE != block_write (ychild, true)
    || DONE != block_write (zchild, true)
    || DONE != block_write (parent, true) )
  { result = FAIL;
    // goto end;
  }
  //-----------------------------------------*/
SPLIT_FREE:;
#ifdef  DEBUG
  block_print_dbg (parent, "parent");
  block_print_dbg (ychild, "ychild");
  block_print_dbg (zchild, "zchild");
#endif
  //-----------------------------------------
  return result;
}
eDBState  block_merge_child (IN sBlock *parent, OUT sBlock *lchild, IN  sBlock *rchild, IN const sDBT *key)
{ /*-----------------------------------------
   *  x: ..k..      x: .....
   *      / \   =>       /
   *     y   z         ykz
   *-----------------------------------------*/
  sDBT val = block_key_data (parent, key);

  uint_t  need_size = (rchild->free_ - rchild->data_) + sizeof (uint_t);
  uint_t  need_elsz =  val.size + key->size + 3U * sizeof (uint_t);
  if ( lchild->head_->free_size_ < need_size + need_elsz )
    return FAIL;

  block_insert (lchild, key, &val, NULL, 0 /* ??? */ );
  block_delete (parent, key);
  //-----------------------------------------
  memcpy (lchild->free_, rchild->data_ - sizeof (uint_t), need_size);
  //-----------------------------------------
  lchild->free_             += need_size;
  lchild->head_->free_size_ -= need_size;
  //-----------------------------------------
  return DONE;
}
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
    if ( offset != MYDB_OFFSET2NEW && offset < db->head_.block_count_ )
    {
      block->offset_ = offset;
      if ( DONE != block_read (block) )
      {
        fail = true;
        mydb_errno = MYDB_ERR_FPARAM;
        fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
        goto BLOCK_FREE;
      }
    }
    else if ( offset != MYDB_OFFSET2NEW )
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

      block->head_->free_size_ = block->size_ - sizeof (sDBHB);
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
