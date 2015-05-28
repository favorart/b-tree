#include "stdafx.h"
#include "mydb_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"
#include "mydb_cache.h"
#include "mydb_log.h"

//-------------------------------------------------------------------------------------------------------------
int  mydb_head_sync (IN sDB *db)
{
  //-----------------------------------------
  if ( lseek (db->hfile_, 0L, SEEK_SET)
    || write (db->hfile_, &db->head_, sizeof (sDBFH)) != sizeof (sDBFH) )
  { fprintf (stderr, "mydb header%s\n", strerror (errno));
    return 1;
  }
  //-----------------------------------------
  return 0;
}
//-------------------------------------------------------------------------------------------------------------
/* Open DB, if it exists, otherwise create DB */
sDB* mydb_create (IN const char *file, IN const sDBC *conf)
{
  const char *error_prefix = "mydb creation";
  bool  fail = false;
  //-----------------------------------------
  if ( conf->page_size <= (2U * sizeof (sDBFH)) )
  {
    mydb_errno = MYDB_ERR_FPARAM;
    fprintf (stderr, "%s%s page_size <= (page_header = %d).\n",
             error_prefix, strmyerror (mydb_errno), sizeof (sDBFH));
    fail = true;
    goto CREAT_FREE;
  }

  sDB  *db = NULL;
  if ( !(db = calloc (1U, sizeof (sDB))) )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto CREAT_FREE;
  }
  //-----------------------------------------
  db->close  = &mydb_close;
  db->delete = &mydb_delete;
  db->select = &mydb_select;
  db->insert = &mydb_insert;
  db->sync   = &mydb_flush;
  //-----------------------------------------
  db->hfile_ = open (file, O_CREAT | O_TRUNC | O_RDWR | O_BINARY, S_IREAD | S_IWRITE);
  if ( db->hfile_ == -1 )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto CREAT_FREE;
  }
  //-----------------------------------------
  db->head_.db_size_     =  conf->db_size;
  db->head_.page_size_   =  conf->page_size;
  db->head_.cache_size_  =  conf->cache_size;
  db->head_.block_count_ = (conf->db_size - sizeof (sDBFH)) / conf->page_size;
  db->head_.nodes_count_ = (0U);
  db->head_.techb_count_ = (db->head_.block_count_ - 1U) / MYDB_BITSINBYTE + 1U; /* round to large integer */
  db->head_.offset2root_ = MYDB_OFFSET2NEW; /* first non-tech.block */
  
  //-----------------------------------------
  long  mydb_file_size = (db->head_.block_count_ * conf->page_size - 1U);

  uchar_t  last_file_byte = 0;
  /* (File-Header + Memory-Blocks) allocate*/
  if ( write (db->hfile_, &db->head_, sizeof (sDBFH)) != sizeof (sDBFH)
    || lseek (db->hfile_,  mydb_file_size, SEEK_CUR ) != mydb_file_size + sizeof (sDBFH)
    || write (db->hfile_, &last_file_byte, sizeof (uchar_t)) != sizeof (uchar_t) )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto CREAT_FREE;
  }

  /* Seek blocks set, after header */
  lseek (db->hfile_, sizeof (sDBFH), SEEK_SET);
  //-----------------------------------------
#ifndef MYDB_NOCACHE
  if ( mydb_cache_init (db) )
  {
    fail = true;
    goto CREAT_FREE;
  }
#endif // !MYDB_NOCACHE
  //-----------------------------------------
  db->techb_array_ = calloc (db->head_.techb_count_, sizeof (sTechB));
  if ( !db->techb_array_ )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto CREAT_FREE;
  }

  for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
  {
    if ( !techb_create (db, (uchar_t*) &db->techb_array_[i], i) )
    {
      fail = true;
      goto CREAT_FREE;
    }
  }
  //-----------------------------------------
  db->root_ = block_create (db, MYDB_OFFSET2NEW);
  if ( !db->root_ )
  {
    fail = true;
    goto CREAT_FREE;
  }
  else
  {
    (*block_type (db->root_)) = Leaf;
    db->head_.offset2root_ = db->root_->offset_;
  }
  //-----------------------------------------
  if ( mydb_head_sync (db) )
  {
    fail = true;
    goto CREAT_FREE;
  }

#ifndef MYDB_NOCACHE
  /* Write ahead log */
  if ( !(db->log_ = mydb_log_open (true)) )
  { fail = true; }
#endif // !MYDB_NOCACHE
  //-----------------------------------------
CREAT_FREE:
  if ( fail )
  { db->close (db);
    db = NULL;
  }
  //-----------------------------------------
  return db;
}
sDB* mydb_open   (IN const char *file)
{
  const char *error_prefix = "mydb open";
  bool  fail = false;
  bool  file_exists = false;
  //-----------------------------------------
  sDB   *db = NULL;
  if ( !(db = calloc (1U, sizeof (sDB))) )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto OPEN_FREE;
  }
  //-----------------------------------------
  db->close  = &mydb_close;
  db->delete = &mydb_delete;
  db->select = &mydb_select;
  db->insert = &mydb_insert;
  db->sync   = &mydb_flush;
  //-----------------------------------------
  /* Check for db file existence */
  if ( access (file, F_OK | R_OK | W_OK) == -1 )
  {
    // file_exists = false;
    mydb_errno = MYDB_ERR_FNEXST;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    fail = true;
    goto OPEN_FREE;
  }

  //-----------------------------------------
  db->hfile_ = open (file, O_RDWR | O_BINARY, S_IREAD | S_IWRITE);
  if ( db->hfile_ == -1 )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto OPEN_FREE;
  }

  //-----------------------------------------
  if ( read (db->hfile_, &db->head_, sizeof (sDBFH)) != sizeof (sDBFH) )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto OPEN_FREE;
  }

#ifndef MYDB_NOCACHE
  if ( mydb_cache_init (db) )
  {
    fail = true;
    goto OPEN_FREE;
  }
#endif // !MYDB_NOCACHE
  //-----------------------------------------
  db->techb_array_ = calloc (db->head_.techb_count_, sizeof (sTechB));
  if ( !db->techb_array_ )
  {
    fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
    fail = true;
    goto OPEN_FREE;
  }

  for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
  {
    if ( !techb_create (db, (uchar_t*) &db->techb_array_[i], i) )
    {
      fail = true;
      goto OPEN_FREE;
    }
  }
  //-----------------------------------------
  db->root_ = block_create (db, db->head_.offset2root_);
  if ( !db->root_ )
  {
    fail = true;
    goto OPEN_FREE;
  }

#ifndef MYDB_NOCACHE
  //-----------------------------------------
  /*   В случае crash'a приложения, db_open ()
   *   обязана уметь восстанавливать последнее
   *   состояние B-tree по журналу.
   */
  if ( mydb_recover (db) )
  { fail = true; }
#endif // !MYDB_NOCACHE
  //-----------------------------------------
OPEN_FREE:;
  if ( fail )
  {
    db->close (db);
    db = NULL;
  }
  //-----------------------------------------
  return db;
}

int  mydb_close  (IN sDB *db)
{
  if ( db )
  {
    if ( db->cache_ )
    { mydb_flush (db); }
    //-----------------------------------------
    techb_sync (db);
    for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
    { techb_destroy (&db->techb_array_[i]); }

    free (db->techb_array_);
    db->techb_array_ = NULL;
    //-----------------------------------------
    block_destroy (db->root_);
    close (db->hfile_);
    //-----------------------------------------
#ifndef MYDB_NOCACHE
    if ( db->cache_ )
    { mydb_cache_free (db); }

    if ( db->log_ )
    {
      mydb_logging   (db, LOG_DB_CLS, NULL, NULL);
      mydb_log_close (db);
    }
#endif // !MYDB_NOCACHE
    //-----------------------------------------
    free (db);
  }
  return 0;
}
int  mydb_delete (IN sDB *db, IN const sDBT *key)
{
  //-----------------------------------------
#ifndef MYDB_NOCACHE
  /* Write ahead log */
  mydb_logging (db, LOG_DELETE, key, NULL);
#endif // !MYDB_NOCACHE
  //-----------------------------------------
  return block_delete_deep (db->root_, key)
      || mydb_head_sync (db);
}
int  mydb_insert (IN sDB *db, IN const sDBT *key, IN const sDBT *data)
{
#ifdef _DEBUG
  char str[100];
  memcpy (str, key->data, key->size);
  str[key->size] = '\0';
  printf ("k='%s'\n", str);
#endif // _DEBUG

  //-----------------------------------------
#ifndef MYDB_NOCACHE
  /* Write ahead log */
  mydb_logging (db, LOG_INSERT, key, data);
#endif // !MYDB_NOCACHE

  int  result = 0;
  sBlock *rb = db->root_;

#ifdef _DEBUG
  block_print_dbg (rb, "root");
#endif // _DEBUG
  //-----------------------------------------
  if ( block_isfull (rb) )
  {
    sBlock *new_root, *new_block;
    /* alloc new free blocks */
    new_root  = block_create (db, MYDB_OFFSET2NEW);
    new_block = block_create (db, MYDB_OFFSET2NEW);
    if ( !new_root || !new_block )
      return -1;

    /* block type of rb was Pass or Leaf */
    (*block_type (new_block)) = (*block_type (rb));

    (*block_type (new_root)) = Pass; // Root
    (*block_nkvs (new_root)) = 0;

    db->root_ = new_root;
    db->head_.offset2root_ = new_root->offset_;    
    db->root_->head_->pointer_0_ = rb->offset_;

    if ( block_split_child (new_root, rb, new_block)
      || block_add_nonfull (new_root, key, data) )
        result = -1;
    //-----------------------------------------
    block_destroy (rb);
    block_destroy (new_block);
    //-----------------------------------------
  }
  else
  {
    result = block_add_nonfull (rb, key, data);
  }
  //-----------------------------------------
  return result || mydb_head_sync (db);
}
int  mydb_select (IN sDB *db, IN const sDBT *key, OUT      sDBT *data)
{ return block_select_deep (db->root_, key, data); }
int  mydb_flush  (IN sDB *db)
{
  //-----------------------------------------
#ifndef MYDB_NOCACHE
  /* cache: disk syncronization */
  mydb_cache_sync (db);
  mydb_logging    (db, LOG_CHKPNT, NULL, NULL);
#endif // !MYDB_NOCACHE
  //-----------------------------------------
  return mydb_head_sync (db);
}
//-------------------------------------------------------------------------------------------------------------
