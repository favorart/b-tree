#include "stdafx.h"
#include "block.h"

//-------------------------------------------------------------------------------------------------------------
uint_t     get_free_block (IN sDB    *db)
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
bool       block_is_full  (IN sBlock *block)
{ return (2U * block->head_->free_size_ >= block->size_); }
//-------------------------------------------------------------------------------------------------------------
sDBT*      block_key_next (IN sBlock *block, IN sDBT *key, OUT uint_t *vsz)
{
 if ( !key ) // get first
 {
  uint_t* ksz = block->data_;
  if( vsz ) *vsz = *(ksz + 1U);

  key->size = *ksz;
  key->data = (void*) (ksz + 2U);
  return key; 
 } // !!!
 else // get next
 {
  // uint_t* ksz =  ;
  uint_t* vsz = (uchar_t*) key->data - 1;

   if ( key->data > (void*)(block->memory_ + (block->size_ - block->head_->free_data_) ) // get last
   {
    return NULL;
   }
 
 } // !!!
}
uint_t*    block_key_ptr  (IN sBlock *block, IN sDBT *key)
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
uint_t*    block_key_val  (IN sBlock *block, IN sDBT *key, OUT void** value)
{
 if ( !key ) return NULL;
 
 uint_t *vsz = (uint_t*) (key->data) - 1U;
 *value = ((uchar_t*) (key->data) + key->size);

 return vsz;
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_insert   (IN sBlock *block, IN sDBT *key, IN  sDBT *value)
{
 if ( !(*block_nkvs (block)) )
 {
  uint_t pointer1 = 1;
  memset (block->data_, pointer1, sizeof (uint_t));
 }

 sDBT *k = block_key_next (block, NULL);
 while ( k && key < k )
 {
  // x[i + 1].key = x[i].key;
  k = block_key_next (block, k);
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
eDBState   block_select   (IN sBlock *block, IN sDBT *key, OUT sDBT *value)
{
 eDBState result = DONE;

 sDBT* k = block_key_next (block, NULL);
 while ( key > k )
 {
  k = block_key_next (block, k);
 }

 if ( k && !key_compare (key, k) )
 {
  void  *val = NULL;
  uint_t vsz = block_key_val (block, k, &val);

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
  sBlock *child = block_create (block->owner_db_, (*block_key_ptr (block, k)) );
  result = block_select (child, key, value);
  block_destroy (child);
 }
 return result;
}
eDBState   block_delete   (IN sBlock *block, IN sDBT *key)
{ 
 
 
 
 return DONE;
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_seek     (IN sDB    *db, IN uint32_t index, IN bool mem_block)
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
 //-----------------------------------------
 if ( block->head_->db_offset_ >= block->owner_db_->head_.block_count_ )
  return FAIL;
 
 if ( DONE == block_seek (block->owner_db_, block->head_->db_offset_, true) )
 {
  // checking for reading successfull
  if( read (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
   return FAIL;
 }
 //-----------------------------------------
 lseek (block->owner_db_->hfile_, file_position, SEEK_SET);

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
sBlock*    block_create   (IN sDB    *db, IN uint_t  offset)
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
  block->data_ = (block->memory_ + sizeof (sDBFH));

  if ( offset )
   block->head_->db_offset_ = offset;
  else
   block->head_->db_offset_ = get_free_block (db);

  if ( block_read (block) == FAIL )
  {
   errno = EINVAL;
   result = true;
   // goto end;
  }
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
 free (block->memory_);
 free (block);
}
//-------------------------------------------------------------------------------------------------------------
void       block_split_child (sBlock *parent, uint_t offset)
{
 // uint_t i, j;
 // 
 sBlock* extra = block_create (parent->owner_db_, 0U);
 sBlock* child = block_create (parent->owner_db_, offset);  /* FULL NODE */

 (*block_type (extra)) = (*block_type (child));
 (*block_nkvs (extra)) = (*block_nkvs (child)) / 2U;

 /* Claculate the number of items to deligate */
 uint_t size = 0U, count = 0U;
 sDBT* k = block_key_next (child, NULL);
 while ( 2U * size <= child->head_->free_size_ )
 {
  ++count;
  size += k->size + *block_key_val ()
  k = block_key_next (child, k);
 }




 // // extra.keys_copy (&extra, extra.key_get (&extra, 0), child->key_get (child, t), t - 1);
 // // // for ( uint_t i = 0U; i < (t - 1U); ++i )
 // // //  // shift keys
 // // //  // + value
 // // //  extra[i] = child[i + t];
 // 
 // if ( *child->type (child) != Leaf )
 //  for ( i = 0U; i < t; ++i )
 //   *extra->pointer (extra, i) = *child->pointer (child, i + t);
 // *child->nkvs (child) = t - 1;
 // 
 // for ( j = parent->nkvs (parent); j > i; --j )
 //  *(parent->pointer (parent, j + 1)) = *(parent->pointer (parent, j));
 // *(parent->pointer (parent, i + 1)) = extra->db_offset_;
 // 
 // 
 // 
 // // parent->keys_shift (parent, i, parent->nkeys (parent) - 1, 0);
 // // for ( uint_t j = ; j <= i; --j )
 // //  // shift keys
 // //  // + value
 // //  parent[j+1] = parent[j];
 // {
 //  sDBT *key, *value;
 //  child->key_get (child, t, &key, &value);
 //  parent->insert (parent, key, value);
 //  child->delete (child, key);
 //  //parent[i].key = y[t].key;
 // }
 // child->write (&child);
 // extra->write (&extra);
 // parent->write (parent);
 // 
 // return;
}
void       block_add_nonfull (sBlock *block, sDBT *key, sDBT *value)
{
 
 if ( (*block_type (block)) == Leaf )
 {
  block_insert (block, key, value);
  block_write  (block);
 }
 else
 {
  sDBT *k = block_key_next (block, NULL); // !!! key_prev
  while ( k && key > k )
  {
   k = block_key_next (block, k);
  }
  
  sBlock *child = block_create (block->owner_db_, *block_key_ptr (block, k));

  // while ( i >= 1 && k < x[i].key )
  //  --i;
  //read_block (x[i].pointer);

  // if ( child->full (child) )
  // {
  //  block_split_child (child);
  //  if ( key > k )
  //   ++i;
  // }
  // block_add_nonfull (child, key, value);
 }
}
//-------------------------------------------------------------------------------------------------------------