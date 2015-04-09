#include "stdafx.h"
#include "block.h"
#include "techb.h"

//-------------------------------------------------------------------------------------------------------------
uint_t     block_free     (IN sDB    *db)
{
  uint_t offset = techb_get_bit (db, 0, true);
  db->head_.nodes_count_ += 1;

  if ( DONE != techb_set_bit (db, offset, true) )
  {
    // mydb_errno =
    fprintf (stderr, "%s%s\n", strmyerror (mydb_errno));
    // return ??
    offset = db->head_.block_count_;
  }
 
 return offset;
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
/*
 *   Удаление ключа из внутреннего узла
 *   
 *   Рассмотрим удаление из внутреннего узла. Имеется внутренний узел x
 *   и ключ, который нужно удалить, k. Если дочерний узел, предшествующий
 *   ключу k, содержит больше t - 1 ключа, то находим k_1 – предшественника
 *   k в поддереве этого узла. Удаляем его. Заменяем k в исходном узле на k_1.
 *   Проделываем аналогичную работу, если дочерний узел, следующий за ключом k,
 *   имеет больше t - 1 ключа. Если оба (следующий и предшествующий дочерние узлы)
 *   имеют по t - 1 ключу, то объединяем этих детей, переносим в них k, а далее
 *   удаляем k из нового узла. Если сливаются 2 последних потомка корня – то они
 *   становятся корнем, а предыдущий корень освобождается.
 */
eDBState   block_delete   (IN sBlock *block, IN const sDBT *key)
{ 
 
 
 
 return DONE;
}
//-------------------------------------------------------------------------------------------------------------
eDBState   block_seek     (IN sBlock *block, IN bool mem)
{
  //-----------------------------------------
  long offset = block->head_->db_offset_
              * block->owner_db_->head_.page_size_ + sizeof (sDBFH);
  if ( mem )
    offset += block->owner_db_->head_.techb_count_;
  //-----------------------------------------
  /* lseek returns the offset, in bytes, of the new position from the beginning of the file */
  return (lseek (block->owner_db_->hfile_, offset, SEEK_SET) == offset) ? DONE : FAIL;
}
eDBState   block_read     (IN sBlock *block)
{
  //-----------------------------------------
  long file_position = tell (block->owner_db_->hfile_);
  if ( file_position == -1L)
   return FAIL;
  //-----------------------------------------
  if ( block->head_->db_offset_ >= block->owner_db_->head_.block_count_ )
   return FAIL;
  
  if ( DONE != block_seek (block, true) )
   return FAIL;
  
  if ( read (block->owner_db_->hfile_, block->memory_, block->size_) != block->size_ )
   return FAIL;
  
  lseek (block->owner_db_->hfile_, file_position, SEEK_SET);
  //-----------------------------------------
  return DONE;
}
eDBState   block_write    (IN sBlock *block, IN bool mem)
{
 if ( DONE == block_seek (block, mem)
   && write (block->owner_db_->hfile_, block->memory_, block->size_) == block->size_ )
  return DONE; 
 return FAIL;
}
//-------------------------------------------------------------------------------------------------------------
sBlock*    block_create   (IN sDB    *db, IN uint_t offset)
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
      block->head_->db_offset_ = block_free (db);
      block->head_->free_size_ = block->size_ - sizeof (sDBFH);
      block->head_->kvs_count_ = 0U;
      block->head_->node_type_ = Free;
      block->head_->pointer_0_ = 0U;
    }

    block->offset_ = block->head_->db_offset_;
    block->data_ = (block->memory_ + sizeof (sDBFH));
    block->free_ = (block->memory_ + (block->size_ - block->head_->free_size_));
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
      block_write (block, true) )
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
       || block_write (child,  true) == FAIL
       || block_write (nchild, true) == FAIL )
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

 if ( block_write (child, true) == FAIL ||
      block_write (extra, true) == FAIL ||
      block_write (block, true) == FAIL )
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