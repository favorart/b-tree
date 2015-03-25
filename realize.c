#include "stdafx.h"
#include "block.h"
#include "techb.h"
//-------------------------------------------------------------------------------------------------------------
/*
 * СТРУКТУРА ФАЙЛА БАЗЫ ДАННЫХ
 *
 * N - размер файла; K - число блоков
 * K * 1 бит = M - техническая информация - битовая маска свободных блоков
 * (N - M) - количество памяти для хранения информации
 */
 //-------------------------------------------------------------------------------------------------------------
/*
 *  1 = (this_key >  that_key)
 *  0 = (this_key == that_key)
 * -1 = (this_key <  that_key)
 */
int  key_compare (IN sDBT *this_key, IN sDBT *that_key)
{ if ( this_key->size != that_key->size )
   return (this_key->size > that_key->size) ? 1 : -1;
  return  memcmp (this_key->data, that_key->data, this_key->size); // !!!
                // (this_key->size <= that_key->size) ?
                //  this_key->size :  that_key->size);
}
//-------------------------------------------------------------------------------------------------------------
int  db_close  (IN sDB *db);
int  db_delete (IN sDB *db, IN sDBT *key);
int  db_insert (IN sDB *db, IN sDBT *key, IN  sDBT *data);
int  db_select (IN sDB *db, IN sDBT *key, OUT sDBT *data);
int  db_sync   (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
sDB* db_open   (IN const char *file,
                IN const sDBC *conf)
{
 eDBState  result = DONE;
 int  file_exists = 0;

  sDB *db = (sDB*) calloc (1U, sizeof (sDB));
  if ( db )
  {
   db->close  = &db_close;
   db->delete = &db_delete;
   db->select = &db_select;
   db->insert = &db_insert;
   db->sync   = &db_sync;

   db->root_   = block_create (db, 0U);
   db->parent_ = NULL;
   db->child_  = NULL;
   db->extra_  = NULL;

   (*block_type (db->root_)) = Root;
   (*block_nkvs (db->root_)) = 0U;

   /* Check for existence */
   if ( (access (file, 0)) != -1 )
    errno = ENOENT;
   else
    file_exists = 1;

   db->hfile_ = open (file, _O_RDWR | _O_BINARY | _O_NOINHERIT);
   if ( !db->hfile_ )
    goto finish;

   if ( file_exists ) /* open existing file */
   {
    if ( read (db->hfile_,
              &db->head_,
               sizeof (sDBC)) != sizeof (sDBC) )
    {
     result = FAIL;
     goto finish;
    } // end if read

    if ( db->head_.db_size_   != conf->db_size
      || db->head_.page_size_ != conf->page_size
      /* !!! cache !!! */ )
    {
     result = FAIL;
     errno = EINVAL;
     goto finish;
    } // end if conf
   } // end if file_exists
   else /* if !file_exists - create a new file */
   {
    db->head_.db_size_   = conf->db_size;
    db->head_.page_size_ = conf->page_size;
    db->head_.block_count_ = conf->db_size / conf->page_size;
    db->head_.nodes_count_ = 0U;
    db->head_.techb_count_ = db->head_.block_count_ / BITSINBYTE;
    // db->head_. = db->all_blks_count_ - db->tch_blks_count_;
    db->head_.offset2root_ = 0U; // !!!

    /* File Header */
    write (db->hfile_, conf, sizeof (sDBFH));
    /* Memory blocks */
    lseek (db->hfile_, (db->head_.block_count_ - 1U) * conf->page_size, SEEK_SET);
    write (db->hfile_, (db->root_->memory_), conf->page_size);
    /* Seek blocks set, after header */
    lseek (db->hfile_, sizeof (sDBFH), SEEK_SET);
   } // end else

   db->techb_arr_ = calloc (db->head_.techb_count_, sizeof (sTechB));
   for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
    techb_create (db, (uchar_t*) &db->techb_arr_[i], i);

  } // end if db
  else
  {
   result = FAIL;
   errno = ENOMEM;
   // goto finish;
  }
  //-----------------------
finish:
  if ( result ) // == FAIL
  { db->close (db); }
  //-----------------------
  return db;
}
int  db_close  (IN sDB *db)
{
 //-----------------------
 for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
 {
  techb_sync    (db);
  techb_destroy (&db->techb_arr_[i]);
 }
 free (db->techb_arr_);

 block_write   (db->root_);
 block_destroy (db->root_);

 close (db->hfile_);
 free  (db);
 //-----------------------
 // db = NULL;
 return 0;
}
int  db_delete (IN sDB *db, IN sDBT *key)
{
 
 return 0;
}
int  db_insert (IN sDB *db, IN sDBT *key, IN  sDBT *data)
{
 sBlock *rb = db->root_;
 if ( block_is_full (rb) )
 {
  sBlock *new_root = block_create (db, 0);
  
  db->root_ = new_root;

  (*block_type    (rb))       = Pass;
  (*block_type    (new_root)) = Root;
  (*block_nkvs    (new_root)) = 0;
  (*block_pointer (new_root, 0)) = rb->head_->db_offset_;
 
  block_split_child (new_root, 1, db);
  block_add_nonfull (new_root, key, data, db);
 }
 else
 {
  block_add_nonfull (rb, key, data, db);
 }

 return 0;
}
int  db_select (IN sDB *db, IN sDBT *key, OUT sDBT *data)
{ return block_select (db->root_, key, data); }
int  db_sync   (IN sDB *db)
{
 /* cache */
 return 0;
}
//-------------------------------------------------------------------------------------------------------------

