#include "stdafx.h"
#include "block.h"
#include "techb.h"

//-------------------------------------------------------------------------------------------------------------
/* СТРУКТУРА ФАЙЛА БАЗЫ ДАННЫХ
 *
 * N - размер файла; K - число блоков
 * K * 1 бит = M - техническая информация - битовая маска свободных блоков
 * (N - M) - количество памяти для хранения информации
 */
 //-------------------------------------------------------------------------------------------------------------
/*  1 = (this_key >  that_key); 0 = (this_key == that_key); -1 = (this_key <  that_key)  */
int  key_compare (IN const sDBT *this_key, IN const sDBT *that_key)
{ // if ( this_key->size != that_key->size )
  //  return (this_key->size > that_key->size) ? 1 : -1;
  return  memcmp (this_key->data, that_key->data, // this_key->size);
                 (this_key->size <= that_key->size) ?
                  this_key->size :  that_key->size);
}
//-------------------------------------------------------------------------------------------------------------
int  db_close  (IN sDB *db);
int  db_delete (IN sDB *db, IN const sDBT *key);
int  db_insert (IN sDB *db, IN const sDBT *key, IN  const sDBT *data);
int  db_select (IN sDB *db, IN const sDBT *key, OUT       sDBT *data);
int  db_sync   (IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
sDB* db_open   (IN const char *file, IN const sDBC *conf)
{
  const char *error_prefix = "mydb creation";
  bool  fail = false;
  bool  file_exists = false;
  sDB  *db = NULL;
  //-----------------------------------------
  if ( conf->page_size <= sizeof (sDBFH) )
  {
    fail = true;
    mydb_errno = MYDB_ERR_FPARAM;
    fprintf (stderr, "%s%s page_size <= page_header = %d.\n",
             error_prefix, strmyerror (mydb_errno), sizeof (sDBFH));
    goto MYDB_FREE;
  }

  db = (sDB*) calloc (1U, sizeof (sDB));
  if ( !db )
  {
    fail = true;
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    goto MYDB_FREE;
  }

  db->close  = &db_close;
  db->delete = &db_delete;
  db->select = &db_select;
  db->insert = &db_insert;
  db->sync   = &db_sync;

  /* Check for db file existence */
  if ( // access (file, F_OK) != -1 && // exist
          access (file, R_OK | W_OK) != -1 )
  {
    file_exists = true;
  }
  else
  {
    mydb_errno = MYDB_ERR_FNEXST;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
  }

  db->hfile_ = _open (file, O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
  if ( db->hfile_ == -1 )
  {
    fail = true;
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    goto MYDB_FREE;
  }

  if ( file_exists ) /* open existing file */
  {
    if ( read (db->hfile_, &db->head_, sizeof (sDBC)) != sizeof (sDBC) )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto MYDB_FREE;
    } // end if read

    if ( db->head_.db_size_   != conf->db_size
      || db->head_.page_size_ != conf->page_size )
    {
      fail = true;
      mydb_errno = MYDB_ERR_FPARAM;
      fprintf (stderr, "%s%s file mydb config is not equal actual.\n",
               error_prefix, strmyerror (mydb_errno));
      goto MYDB_FREE;
    } // end if conf
  } // end if file_exists
  else /* if !file_exists - create a new file */
  {
    uchar_t  c = 0U;
    long  mydb_file_size = 0L;

    db->head_.db_size_     = conf->db_size;
    db->head_.page_size_   = conf->page_size;
    db->head_.block_count_ = conf->db_size / conf->page_size;
    db->head_.nodes_count_ = 0U;
    db->head_.techb_count_ = db->head_.block_count_ / BITSINBYTE;
    db->head_.offset2root_ = db->head_.techb_count_; /* first non tech block */

    mydb_file_size = (db->head_.block_count_) * conf->page_size - 1U;

    /* (File-Header + Memory-Blocks) allocate*/
    if ( write (db->hfile_, conf, sizeof (sDBFH)) != sizeof (sDBFH)
      || lseek (db->hfile_, mydb_file_size, SEEK_SET) != mydb_file_size
      || write (db->hfile_, &c, 1U) != 1U )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto MYDB_FREE;
    }
    /* Seek blocks set, after header */
    lseek (db->hfile_, sizeof (sDBFH), SEEK_SET);
  } // end else file_exists

  db->techb_arr_ = calloc (db->head_.techb_count_, sizeof (sTechB));
  if ( !db->techb_arr_ )
  {
    fail = true;
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    goto MYDB_FREE;
  }

  for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
  {
    if ( !techb_create (db, (uchar_t*) &db->techb_arr_[i], i) )
    {
      fail = true;
      goto MYDB_FREE;
    }
  }

  db->parent_ = NULL;
  db->child_ = NULL;
  db->extra_ = NULL;

  db->root_ = block_create (db, 0U);
  if( !db->root_ )
  {
    fail = true;
    goto MYDB_FREE;
  }
  
  (*block_type (db->root_)) = Root;
  (*block_nkvs (db->root_)) = 0U;
  //-----------------------
MYDB_FREE:
  if ( fail )
  {
    db->close (db);
    db = NULL;
  }
  //-----------------------
  return db;
}
int  db_close  (IN sDB *db)
{
  if ( db )
  {
    //-----------------------
    if ( db->techb_arr_ )
    {
      techb_sync (db);
      for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
      {
        techb_destroy (&db->techb_arr_[i]);
      }
      free (db->techb_arr_);
      db->techb_arr_ = NULL;
    }
    //-----------------------
    if ( db->hfile_ != -1 )
      block_write (db->root_, true);
    block_destroy (db->root_);
    //-----------------------
    if ( db->hfile_ != -1 )
    {
      close (db->hfile_);
      db->hfile_ = -1;
    }
    free (db);
    //-----------------------
  }
  return 0;
}
int  db_delete (IN sDB *db, IN const sDBT *key)
{
 
 return 0;
}
int  db_insert (IN sDB *db, IN const sDBT *key, IN  const sDBT *data)
{
 sBlock *rb = db->root_;
 if ( block_is_full (rb) )
 {
  sBlock *new_root = block_create (db, 0);
  
  db->root_ = new_root;

  (*block_type (rb))       = Pass;
  (*block_type (new_root)) = Root;
  (*block_nkvs (new_root)) = 0;
  (*block_ptr  (new_root, NULL)) = rb->head_->db_offset_;
 
  block_split_child (new_root, 1);
  block_add_nonfull (new_root, key, data);
 }
 else
 {
  block_add_nonfull (rb, key, data);
 }

 return 0;
}
int  db_select (IN sDB *db, IN const sDBT *key, OUT       sDBT *data)
{ return block_select (db->root_, key, data); }
int  db_sync   (IN sDB *db)
{
  /* cache */
  return 0;
}
//-------------------------------------------------------------------------------------------------------------

