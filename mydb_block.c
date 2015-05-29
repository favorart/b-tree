#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"
#include "mydb_cache.h"

//-------------------------------------------------------------------------------------------------------------
/* DECORATIVE OPERATIONS */

eDBNT*    block_type (IN sBlock *block)
{ return  &block->head_->node_type_; }
uint_t*   block_nkvs (IN sBlock *block)
{ return  &block->head_->kvs_count_; }
uint_t*   block_lptr (IN sBlock *block, IN const sDBT *key)
{
#ifdef _DEBUG
  if ( key && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_) )
  { fprintf (stderr, "memory block left ptr%s", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }
#endif // _DEBUG
  /* | ptr | ksz | vsz | key | val | */
  return (key) ? ((uint_t*) key->data - 3U) : ((uint_t*) block->free_ - 1U);
}
uint_t*   block_rptr (IN sBlock *block, IN const sDBT *key)
{
#ifdef _DEBUG
  if ( key && ((uchar_t*) key->data < block->data_ || (uchar_t*) key->data >= block->free_) )
  { fprintf (stderr, "memory block right ptr%s", strmyerror (MYDB_ERR_FPARAM));
    return NULL;
  }
#endif // _DEBUG
  /* | ksz | vsz | key | val | ptr | */
  return (key) ? (uint_t*) ((uchar_t*) key->data + key->size + *((uint_t*) key->data - 1U)) :
    &block->head_->pointer_0_;
}
//-------------------------------------------------------------------------------------------------------------
/* full:   free = (1/4 * size) */
bool      block_isfull (IN const sBlock *block)
{ return ( 4U * block->head_->free_size_ <= 1U * (block->size_ - sizeof (sDBHB)) ); }
/* enough: free = (3/4 * size) */
bool      block_enough (IN const sBlock *block)
{ return ( 4U * block->head_->free_size_ <= 3U * (block->size_ - sizeof (sDBHB)) ); }
//-------------------------------------------------------------------------------------------------------------
/* HIGH-LEVEL OPERATIONS */

eDBState  block_add_nonfull (IN sBlock *block, IN const sDBT *key, IN const sDBT *value)
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
    if ( DONE != block_write (block) )
    {
      result = FAIL;
      fprintf (stderr, "%s%s Offset is %d.\n", error_prefix,
               strmyerror (MYDB_ERR_BWRITE), block->offset_);
    }
  }
  else
  {
    //-----------------------------------------
    /* Passing down through the tree */
    sDBT iter = { 0 }, pre_k = { 0 };
    sDBT   *k = block_key_next (block, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    {
      pre_k = *k;
      k = block_key_next (block, k, NULL);
    }
    //-----------------------------------------
#ifdef _DEBUG
    if ( k )
    {
      char str[100];
      memcpy (str, k->data, k->size);
      str[k->size] = '\0';
      printf ("anf='%s'\n", str);
    }
#endif // _DEBUG

    if ( !(ychild = block_create (block->owner_db_, *block_lptr (block, k))) )
    {
      result = FAIL;
      goto NONFULL_FREE;
    }
    //-----------------------------------------
    /* Keep all block non-full through the path */
    if ( block_isfull (ychild) )
    {
      if ( !(zchild = block_create (block->owner_db_, MYDB_OFFSET2NEW)) )
      {
        result = FAIL;
        fprintf (stderr, "%s%s", error_prefix, strerror (errno));
        goto NONFULL_FREE;
      }
      //-----------------------------------------
      /* split the block, if there is a full one */
      if ( DONE != block_split_child (block, ychild, zchild) )
      {
        result = FAIL;
        goto NONFULL_FREE;
      }

      /* recently inserted key (that splits y and z) */
      sDBT *k = block_key_next (block, &pre_k, NULL);
      if ( k && key_compare (k, key) < 0 )
      { SWAP (&ychild, &zchild); }
    }
    //-----------------------------------------
    /* insert into non-full block */
    if ( DONE != block_add_nonfull (ychild, key, value) )
    {
      result = FAIL;
      goto NONFULL_FREE;
    }
    if ( DONE != block_write (ychild) )
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
  block_destroy (zchild);
  // ychild = NULL; zchild = NULL;
  //-----------------------------------------
  return result;
}
eDBState  block_select_deep (IN sBlock *block, IN const sDBT *key, OUT      sDBT *value)
{
  const char *error_prefix = "memory block searching";
  eDBState    result = DONE;

#ifdef _DEBUG
  block_print_dbg (block, "select");
#endif // _DEBUG
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
#ifdef  _DEBUG
    if ( k )
    {
      char str[100];
      memcpy (str, k->data, k->size);
      str[k->size] = '\0';
      printf ("select: '%s'=", str);

      memcpy (str, key->data, key->size);
      str[key->size] = '\0';
      printf ("='%s'\n", str);
    }
#endif // _DEBUG

    result = FAIL;
    value->data = NULL;
    value->size = 0;
  }
  else
  {
    sBlock *child = block_create (block->owner_db_, *block_lptr (block, k));
    if ( child )
    {
      result = block_select_deep (child, key, value);
      block_destroy (child);
    }
    else result = FAIL;
  }
  //-----------------------------------------
  return result;
}
eDBState  block_delete_deep (IN sBlock *block, IN const sDBT *key)
{
  const char *error_prefix = "memory block deep delete";
  eDBState    result = DONE;
  sDBT         value = { 0 };
  sBlock     *ychild = NULL, *zchild = NULL;
  //-----------------------------------------
  sDBT iter = { 0 }, *k = &iter;
  /* if key is in current block */
  if ( DONE == block_select (block, key, &value, k) )
  {
    if ( (*block_type (block)) == Leaf )
    {
      result = block_delete (block, key, 0);
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
  block_destroy (ychild); 
  block_destroy (zchild);

  // ychild = NULL; zchild = NULL;
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

#ifdef _DEBUG
  char str[100];
  memcpy (str, k->data, k->size);
  str[k->size] = '\0';
  printf ("k='%s'\n", str);
#endif // _DEBUG

  /* move the middle element into the parent block */
  if ( DONE != block_insert (parent, k, &val, NULL, zchild->offset_) )
  {
    result = FAIL;
    goto SPLIT_FREE;
  }

  /* RPtr must by copied to zchild->pointer_0_, not to parent RPtr */
  zchild->head_->pointer_0_ = *block_rptr (ychild, k);

  ++count;
  amount_size += (k->size + val.size) + (3U * sizeof (uint_t));
  //-----------------------------------------
  uint_t bytes_count = ( (ychild->free_ - ychild->data_) - amount_size );
  memcpy (zchild->data_, ychild->data_ + amount_size, bytes_count);

  (*block_nkvs (zchild)) = (*block_nkvs (ychild) - count); /* latest elements of ychild */
  (*block_nkvs (ychild)) = (count - 1U);  /* remaining elements without the middle one */
  //-----------------------------------------
  /* ptr's have already copied with elements data */
  //-----------------------------------------
  zchild->head_->free_size_ -= bytes_count;
  zchild->free_             += bytes_count;

  /* take into consideration the inserted element */
  bytes_count += (k->size + val.size) + (3U * sizeof (uint_t));

  ychild->head_->free_size_ += bytes_count;
  ychild->free_             -= bytes_count;
  memset (ychild->free_, 0, bytes_count);

  // ychild->dirty_ = true; zchild->dirty_ = true;
  //-----------------------------------------
  if ( DONE != block_write (ychild)
    || DONE != block_write (zchild)
    || DONE != block_write (parent) )
  { result = FAIL;
    // goto SPLIT_FREE;
  }
  //-----------------------------------------*/
SPLIT_FREE:;
#ifdef _DEBUG
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
   *      / \   =>      /
   *     y   z        ykz
   *-----------------------------------------*/
  sDBT val = block_key_data (parent, key);

  uint_t  need_size = (rchild->free_ - rchild->data_)  + sizeof (uint_t);
  uint_t  need_else =      (val.size + key->size) + 3U * sizeof (uint_t);
  if ( lchild->head_->free_size_ < (need_size + need_else) )
  {
    fprintf (stderr, "memory block merge%s", strmyerror (MYDB_ERR_NFREES));
    return FAIL;
  }

  // must insert end ->
  block_insert (lchild, key, &val, NULL, MYDB_INVALIDPTR);
  block_delete (parent, key, false);
  //-----------------------------------------
  /* the very left ptr */
  memcpy (lchild->free_ - sizeof (uint_t), rchild->data_ - sizeof (uint_t), need_size);
  //-----------------------------------------
  lchild->free_             += need_size;
  lchild->head_->free_size_ -= need_size;
  
  //-----------------------------------------
  if ( DONE != block_write (lchild)
    || DONE != block_write (rchild)
    || DONE != block_write (parent) )
  {
    return FAIL;
  }
  //-----------------------------------------

  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
#define   block_offset2free  techb_get_index_of_first_free_bit
//-------------------------------------------------------------------------------------------------------------
sBlock*   block_create (IN sDB    *db, IN uint_t offset)
{
  const char *error_prefix = "memory block creation";
  bool fail = false;

  sBlock *block = NULL;
  //-----------------------------------------
  uint_t block_offset;
  if ( offset == MYDB_OFFSET2NEW )
  {
    block_offset = block_offset2free (db);
    if ( block_offset >= db->head_.block_count_ )
    {
      fail = true;
      mydb_errno = MYDB_ERR_NFREEB;
      fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
      goto BLOCK_FREE;
    }
  }
  else { block_offset = offset; }

#ifndef MYDB_NOCACHE
  /* block_read перед тем как прочитать блок с диска, ищет блок в кэше */
  int in_cache = mydb_cache_push (db, block_offset, &block);
  /* returns the placed sBlock & memory */
  if ( !block || !block->memory_ )
  {
    fail = true;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (errno));
    goto BLOCK_FREE;
  }

  if ( !in_cache )
  {
#elif MYDB_NOCACHE
  block = calloc (1U, sizeof (sBlock));
  //-----------------------------------------
  if ( block )
  {
    block->memory_ = calloc (db->head_.page_size_, sizeof (uchar_t));
    if ( !block->memory_ )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto BLOCK_FREE;
    }
#endif // MYDB_NOCACHE

    block->owner_db_ = db;
    block->size_ = db->head_.page_size_;
    block->is_mem_ = true;
    block->head_ = (sDBHB*) block->memory_;
    block->offset_ = block->head_->db_offset_ = block_offset;

    if ( offset == MYDB_OFFSET2NEW )
    {
      block->head_->free_size_ = block->size_ - sizeof (sDBHB);
      block->head_->kvs_count_ = 0U;
      block->head_->node_type_ = Free;
      block->head_->pointer_0_ = MYDB_INVALIDPTR;
    }
    else
    {
      if ( DONE != block_read (block) )
      {
        fail = true;
        mydb_errno = MYDB_ERR_FPARAM;
        fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
        goto BLOCK_FREE;
      }

#ifndef MYDB_NOCACHE
      if ( !block->head_->node_type_ )
      {
#ifdef _DEBUG_CACHE
        mydb_cache_print_debug (db);
#endif // _DEBUG_CACHE

        fail = true;
        mydb_errno = MYDB_ERR_CCHSET;
        fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
        exit (EXIT_FAILURE); // !!!
      }
#endif // !MYDB_NOCACHE
    }

    block->data_ = (block->memory_ + sizeof (sDBHB));
    block->free_ = (block->memory_ + (block->size_ - block->head_->free_size_));

#ifdef MYDB_NOCACHE
  }
  //-----------------------------------------
  else // calloc
  {
    fail = true;
    mydb_errno = MYDB_ERR_FPARAM;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    // goto BLOCK_FREE;
  }
#else // !MYDB_NOCACHE
  }
#endif // !MYDB_NOCACHE

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
void      block_free   (IN sBlock *block)
{
#ifdef MYDB_NOCACHE
  //-----------------------------------------
  if ( block )
    free (block->memory_);
  free (block);
#endif // MYDB_NOCACHE
}
//-------------------------------------------------------------------------------------------------------------
