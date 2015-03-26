#include "stdafx.h"
#include "block.h"

//-------------------------------------------------------------------------------------------------------------
uint_t     block_free     (IN sDB    *db)
{
 // long  file_position = tell (db->hfile_);
 // //-----------------------------------------
 // lseek (db->hfile_, sizeof (sDBFH), SEEK_SET);
 // 
 // for ( uint_t i = 0U; i < (db->head_.page_size_ * BITSINBYTE); ++i )
 // {
 //  if( !(byte &= (1U << (i % BITSINBYTE))) )
 // 
 // }
 // //-----------------------------------------
 // lseek (db->hfile_, file_position, SEEK_SET);
 // 
 // return byte ? NODE : FREE;
 return 0U;
}
//-------------------------------------------------------------------------------------------------------------
eDBNT*     block_type     (IN sBlock *block)
{ return  &block->head_->node_type_; }
uint_t*    block_nkvs     (IN sBlock *block)
{ return  &block->head_->kvs_count_; }
//-------------------------------------------------------------------------------------------------------------
bool       block_is_full  (IN const sBlock *block)
{ return (2U * block->head_->free_size_ <= block->size_); }
//-------------------------------------------------------------------------------------------------------------
sDBT*      block_key_next (IN sBlock *block, IN       sDBT *key, OUT uint_t *vsz)
{
 if ( !key ) // get first
 {
  uint_t* ksz = (uint_t*) block->data_;
  if( vsz ) *vsz = *(ksz + 1U);

  key->size = *ksz;
  key->data = (void*) (ksz + 2U);
  return key; 
 } // !!!
 else // get next
 {
  key->size = *((uchar_t*) key->data + key->size + *((uchar_t*) key->data - sizeof (uint_t)));
  key->data =  ((uchar_t*) key->data + key->size + *((uchar_t*) key->data - sizeof (uint_t)) + sizeof (uint_t) * 2U);

  if ( key->data >= (void*) block->free_ ) // get last
  {
   key->size = 0;
   key->data = NULL;
   if ( vsz )
    *vsz = NULL;
   return NULL;
  }

  if ( vsz ) *vsz = ((uint_t*) key->data - 1);
  return key;
 } // end else
}
uint_t*    block_ptr      (IN sBlock *block, IN const sDBT *key)
{
 if ( !key )
 {
  return block->head_->pointer_0_;
 }
 else
 {
  uint_t *vsz = (uint_t*) (key->data) - 1U;
  // | ksz | vsz | key | val | ptr |
  return (uint_t*) ((uchar_t*) (key->data) + key->size + *vsz);
 }
}
uint_t     block_data     (IN sBlock *block, IN const sDBT *key, OUT void** data)
{
 if ( !key ) return 0;
 
 uint_t *vsz = (uint_t*) (key->data) - 1U;
 *data     = ((uchar_t*) (key->data) + key->size);

 return *vsz;
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_insert   (IN sBlock *block, IN const sDBT *key, IN  const sDBT *value)
{
 if ( !(*block_nkvs (block)) )
 {
  uint_t pointer1 = 1;
  memset (block->data_, pointer1, sizeof (uint_t));
 }

 sDBT *k = block_key_next (block, NULL, NULL);
 while ( k && key < k )
 {
  // x[i + 1].key = x[i].key;
  k = block_key_next (block, k, NULL);
 }
 
 {
  uchar_t *memory = ((uchar_t*)k->data - sizeof (uint_t) * 3);
  // x[i + 1].key = key;
  memmove (memory + sizeof (uint_t) * 3 + value->size + key->size, k, block->head_->free_size_ /* - k*/);
  *(uint_t*) memory = 0; /* ptr */ ++((uint_t*) memory);
  *(uint_t*) memory = key  ->size; ++((uint_t*) memory);
  *(uint_t*) memory = value->size; ++((uint_t*) memory);

  memmove (k, key  ->data, key  ->size); memory += key->size;
  memmove (k, value->data, value->size);
 }

 ++(*(block_nkvs (block)));
 return DONE;
}
eDBState   block_select   (IN sBlock *block, IN const sDBT *key, OUT       sDBT *value)
{
 eDBState result = DONE;

 sDBT* k = block_key_next (block, NULL, NULL);
 while ( key > k )
 {
  k = block_key_next (block, k, NULL);
 }

 if ( k && !key_compare (key, k) )
 {
  void  *val = NULL;
  uint_t vsz = block_data (block, k, &val);

  value->data = malloc (sizeof (uchar_t) * vsz);
  if ( value->data )
  {
   memcpy (value->data, val, vsz);
   value->size = vsz;
  }
  else
   result = FAIL;
 }
 else if ( (*block_type (block)) == Leaf )
 {
  result = FAIL;
  value->data = NULL;
  value->size = 0;
 }
 else
 {
  sBlock *child = block_create (block->owner_db_, (*block_ptr (block, k)) );
  result = block_select (child, key, value);
  block_destroy (child);
 }
 return result;
}
eDBState   block_delete   (IN sBlock *block, IN const sDBT *key)
{ 
 
 
 
 return DONE;
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_seek     (IN const sDB *db, IN uint32_t index, IN bool mem_block)
{
 //-----------------------------------------
 long offset = sizeof (sDBFH) + index * db->head_.page_size_;
 if ( mem_block )
  offset += db->head_.techb_count_;
 //-----------------------------------------
 return (lseek (db->hfile_, offset, SEEK_SET) == offset) ? DONE : FAIL;
}
eDBState   block_read     (IN sBlock *block)
{
 long  file_position = tell (block->owner_db_->hfile_);
 if ( file_position == -1L)
  return FAIL;
 //-----------------------------------------
 if ( block->head_->db_offset_ >= block->owner_db_->head_.block_count_ )
  return FAIL;

 if ( block_seek (block->owner_db_, block->head_->db_offset_, true) == FAIL )
  return FAIL;

 if ( read (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
  return FAIL;
 
 if ( lseek (block->owner_db_->hfile_, file_position, SEEK_SET) != file_position )
  return FAIL;
 //-----------------------------------------
 return DONE;
}
eDBState   block_write    (IN sBlock *block)
{
 if ( DONE == block_seek (block->owner_db_, block->head_->db_offset_, true)
   && write (block->owner_db_->hfile_, block->memory_, block->size_) == block->size_ )
  return DONE; 
 return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
sBlock*    block_create   (IN sDB    *db, IN uint_t offset)
{
 bool result = 0;

 sBlock *block = calloc (1U, sizeof (sBlock));
 if ( block )
 {
  block->owner_db_ = db;
  block->size_ = db->head_.page_size_;

  block->memory_ = calloc (block->size_, sizeof (uchar_t));
  if ( !block->memory_ )
  {
   errno = ENOMEM;
   result = true;
   goto end;
  }

  block->head_ = (sDBHB*) block->memory_;
  if ( offset )
  {
   block->head_->db_offset_ = offset;
   if ( block_read (block) == FAIL )
   {
    errno = EINVAL;
    result = true;
    goto end;
   }
  }
  else
  {
   block->head_->db_offset_ = block_free (db);
   block->head_->free_size_ = block->size_ - sizeof (sDBFH);
   block->head_->kvs_count_ = 0U;
   block->head_->node_type_ = Free;
   block->head_->pointer_0_ = 0U;
  }
  
  block->data_ = (block->memory_ + sizeof (sDBFH));
  block->free_ = (block->memory_ + (block->size_ - block->head_->free_size_));
 }

end:;
 if ( result )
 {
  block_destroy (block);
  block = NULL;
 }
 return block;
}
void       block_destroy  (IN sBlock *block)
{
 if( block )
  free (block->memory_);
 free (block);
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_add_nonfull (IN sBlock *block, IN const sDBT *key, IN const sDBT *value)
{
 eDBState result = DONE;
 if ( (*block_type (block)) == Leaf )
 {
  if ( block_insert (block, key, value) ||
      block_write (block) )
      result = FAIL;
 }
 else
 {
  sDBT *k = block_key_next (block, NULL, NULL);
  while ( k && key > k )
  {
   k = block_key_next (block, k, NULL);
  }

  sBlock *child = block_create (block->owner_db_, *block_ptr (block, k));
  if ( !child )
  {
   result = FAIL;
  }
  else
  {
   if ( block_is_full (child) )
   {
    block_split_child (child, *block_ptr (child, k));
    if ( key > k )
     k = block_key_next (child, k, NULL);
   }
   sBlock *nchild = block_create (child->owner_db_, *block_ptr (child, k));

   if ( !nchild
       || block_add_nonfull (nchild, key, value) == FAIL
       || block_write (child) == FAIL
       || block_write (nchild) == FAIL )
       result = FAIL;

   block_destroy (child);
   block_destroy (nchild);
  }
 }

 return result;
}
eDBState   block_split_child (IN sBlock *block, IN uint_t offset)
{
 eDBState result = DONE;
 sBlock  *parent = block;

 sBlock* extra = block_create (parent->owner_db_, 0U);  /* new free node */
 sBlock* child = block_create (parent->owner_db_, offset);  /* full node */
 if ( !extra || !child )
 {
  result = FAIL;
  goto end;
 }

 (*block_type (extra)) = (*block_type (child));
 // (*block_nkvs (extra)) = (*block_nkvs (child)) / 2U;

 /* Claculate the number of items to deligate */
 uint_t amount_size = 0U, count = 0U, val_size;

 sDBT* k = block_key_next (child, NULL, &val_size);
 while ( 2U * amount_size <= child->head_->free_size_ )
 {
  ++count;
  amount_size += (k->size + val_size) + (3U * sizeof (uint_t));
  k = block_key_next (child, k, &val_size);
 }

 uint_t bnum = ((child->free_ - child->data_) - amount_size);
 memcpy (extra->data_, child->data_ + amount_size, bnum);
 (*block_nkvs (child)) -= count;
 (*block_nkvs (extra))  = count;

 sDBT val = {0};
 val.size = block_data (child, k, &val.data);
 if ( block_insert (parent, k, &val) == FAIL )
 {
  result = FAIL;
  goto end;
 }

 if ( block_write (child) == FAIL ||
      block_write (extra) == FAIL ||
      block_write (block) == FAIL )
 {
  result = FAIL;
  // goto end;
 }

end:;
 block_destroy (child);
 block_destroy (extra);

 return result;
}
//-------------------------------------------------------------------------------------------------------------