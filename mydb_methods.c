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
    || write (db->hfile_, &db->head_, sizeof (sDBFH)) != (ssize_t) sizeof (sDBFH) )
  { fprintf (stderr, "mydb header: %s\n", strerror (errno));
    return 1;
  }
  //-----------------------------------------
  return 0;
}
int  mydb_sync_pages (IN sDB *db)
{
  if ( DONE != techb_sync (db)
    || mydb_cache_sync (db)
    || mydb_head_sync (db)
    || fsync (db->hfile_) )
    return 1;
  return 0;
}
size_t db_max_kv_size (const sDB *db)
{ return block_max_entry (db) - MYDB_ENTRY_OVERHEAD; }
//-------------------------------------------------------------------------------------------------------------
static sDB*  mydb_alloc (void)
{
  sDB *db = calloc (1U, sizeof (sDB));
  if ( !db )
    return NULL;
  //-----------------------------------------
  db->close  = &mydb_close;
  db->delete = &mydb_delete;
  db->select = &mydb_select;
  db->insert = &mydb_insert;
  db->sync   = &mydb_flush;
  db->hfile_ = -1;
  return db;
}
/* cache, bitmap pages and the root: common part of create and open */
static bool  mydb_load (IN sDB *db, IN const char *error_prefix)
{
  if ( mydb_cache_init (db) )
    return false;
  //-----------------------------------------
  db->techb_array_ = calloc (db->head_.techb_count_, sizeof (sTechB));
  if ( !db->techb_array_ )
  {
    fprintf (stderr, "%s: %s\n", error_prefix, strerror (errno));
    return false;
  }
  for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
  {
    if ( !techb_create (db, (uchar_t*) &db->techb_array_[i], i) )
      return false;
  }
  return true;
}
/* the root is re-fetched via the cache on every operation: the pointer is not kept across operations */
static sBlock*  mydb_root (IN sDB *db)
{ return (db->root_ = block_create (db, db->head_.offset2root_)); }
/* keep the root non-full: split it and grow the tree by one level */
static int  mydb_split_root (IN sDB *db)
{
  sBlock *rb = mydb_root (db);
  if ( !rb )
    return -1;
  if ( !block_isfull (rb) )
    return 0;
  //-----------------------------------------
  uint32_t old_root = rb->offset_;
  /* alloc new free blocks */
  sBlock *new_root  = block_create (db, MYDB_OFFSET2NEW);
  sBlock *new_block = block_create (db, MYDB_OFFSET2NEW);
  if ( !new_root || !new_block || !(rb = block_create (db, old_root)) )
    return -1;

  (*block_type (new_root)) = Pass;
  (*block_nkvs (new_root)) = 0;
  new_root->head_->pointer_0_ = old_root;

  db->root_ = new_root;
  db->head_.offset2root_ = new_root->offset_;
  //-----------------------------------------
  return (DONE == block_split_child (new_root, rb, new_block)) ? 0 : -1;
}
//-------------------------------------------------------------------------------------------------------------
sDB* mydb_create (IN const char *file, IN const sDBC *conf)
{
  const char *error_prefix = "mydb creation";
  //-----------------------------------------
  size_t   db_size = (conf && conf->db_size)    ? conf->db_size    : MYDB_DEFAULT_DB_SIZE;
  size_t page_size = (conf && conf->page_size)  ? conf->page_size  : MYDB_DEFAULT_PAGE_SIZE;
  size_t cch_size  = (conf && conf->cache_size) ? conf->cache_size : MYDB_DEFAULT_CACHE_SIZE;

  if ( !file || page_size < MYDB_MIN_PAGE_SIZE || page_size > (1U << 24)
    || db_size > UINT32_MAX || cch_size > UINT32_MAX
    || db_size < sizeof (sDBFH) + 3U * page_size )
  {
    mydb_errno = MYDB_ERR_FPARAM;
    fprintf (stderr, "%s%s page_size >= %u, db_size >= 3 pages and < 4GB.\n",
             error_prefix, strmyerror (mydb_errno), MYDB_MIN_PAGE_SIZE);
    return NULL;
  }
  //-----------------------------------------
  sDB *db = mydb_alloc ();
  if ( !db )
  {
    fprintf (stderr, "%s: %s\n", error_prefix, strerror (errno));
    return NULL;
  }
  //-----------------------------------------
  db->hfile_ = open (file, O_CREAT | O_TRUNC | O_RDWR | O_BINARY, S_IREAD | S_IWRITE);
  if ( db->hfile_ == -1 )
  {
    fprintf (stderr, "%s '%s': %s\n", error_prefix, file, strerror (errno));
    goto CREAT_FAIL;
  }
  //-----------------------------------------
  /* | header | bitmap pages | data pages |, one bit of a bitmap page per data page */
  uint32_t pages = (uint32_t) ((db_size - sizeof (sDBFH)) / page_size);
  uint32_t techb = (uint32_t) ((pages + MYDB_BITSINBYTE * page_size) / (MYDB_BITSINBYTE * page_size + 1U));

  db->head_.magic_       = MYDB_MAGIC;
  db->head_.version_     = MYDB_VERSION;
  db->head_.db_size_     = (uint32_t) db_size;
  db->head_.page_size_   = (uint32_t) page_size;
  db->head_.cache_size_  = (uint32_t) cch_size;
  db->head_.techb_count_ = techb;
  db->head_.block_count_ = pages - techb;
  db->head_.nodes_count_ = 0U;
  db->head_.offset2root_ = MYDB_OFFSET2NEW;
  //-----------------------------------------
  /* (File-Header + Memory-Blocks) allocate */
  if ( ftruncate (db->hfile_, (off_t) sizeof (sDBFH) + (off_t) pages * page_size) )
  {
    fprintf (stderr, "%s: %s\n", error_prefix, strerror (errno));
    goto CREAT_FAIL;
  }
  if ( !mydb_load (db, error_prefix) )
    goto CREAT_FAIL;
  //-----------------------------------------
  /* the empty tree is a single empty leaf */
  db->root_ = block_create (db, MYDB_OFFSET2NEW);
  if ( !db->root_ )
    goto CREAT_FAIL;
  db->head_.offset2root_ = db->root_->offset_;
  //-----------------------------------------
  /* Write ahead log */
  if ( !(db->log_ = mydb_log_open (file, true)) || mydb_flush (db) )
    goto CREAT_FAIL;
  //-----------------------------------------
  return db;

CREAT_FAIL:
  mydb_close (db);
  return NULL;
}
sDB* mydb_open   (IN const char *file)
{
  const char *error_prefix = "mydb open";
  //-----------------------------------------
  /* Check for db file existence */
  if ( !file || access (file, F_OK | R_OK | W_OK) == -1 )
  {
    mydb_errno = MYDB_ERR_FNEXST;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    return NULL;
  }
  //-----------------------------------------
  sDB *db = mydb_alloc ();
  if ( !db )
  {
    fprintf (stderr, "%s: %s\n", error_prefix, strerror (errno));
    return NULL;
  }
  //-----------------------------------------
  db->hfile_ = open (file, O_RDWR | O_BINARY, S_IREAD | S_IWRITE);
  if ( db->hfile_ == -1 )
  {
    fprintf (stderr, "%s '%s': %s\n", error_prefix, file, strerror (errno));
    goto OPEN_FAIL;
  }
  //-----------------------------------------
  if ( read (db->hfile_, &db->head_, sizeof (sDBFH)) != (ssize_t) sizeof (sDBFH)
    || db->head_.magic_ != MYDB_MAGIC || db->head_.version_ != MYDB_VERSION
    || db->head_.page_size_ < MYDB_MIN_PAGE_SIZE
    || db->head_.offset2root_ >= db->head_.block_count_ )
  {
    /* do not overwrite a foreign file on close */
    close (db->hfile_);
    db->hfile_ = -1;
    mydb_errno = MYDB_ERR_FORMAT;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    goto OPEN_FAIL;
  }
  //-----------------------------------------
  if ( !mydb_load (db, error_prefix) || !mydb_root (db) )
    goto OPEN_FAIL;
  //-----------------------------------------
  /*   В случае crash'a приложения, db_open ()
   *   обязана уметь восстанавливать последнее
   *   состояние B-tree по журналу.
   */
  if ( !(db->log_ = mydb_log_open (file, false)) || mydb_recover (db) )
    goto OPEN_FAIL;
  //-----------------------------------------
  return db;

OPEN_FAIL:
  mydb_close (db);
  return NULL;
}
//-------------------------------------------------------------------------------------------------------------
int  mydb_close  (IN sDB *db)
{
  int rc = 0;
  if ( db )
  {
    /* write everything only for a completely opened db */
    if ( db->hfile_ != -1 && db->cache_ && db->techb_array_ && db->root_ && db->log_ )
      rc = mydb_flush (db);
    //-----------------------------------------
    for ( uint_t i = 0U; db->techb_array_ && i < db->head_.techb_count_; ++i )
    { techb_destroy (&db->techb_array_[i]); }
    free (db->techb_array_);
    db->techb_array_ = NULL;
    //-----------------------------------------
    if ( db->hfile_ != -1 )
      close (db->hfile_);
    mydb_cache_free (db);
    mydb_log_close (db);
    //-----------------------------------------
    free (db);
  }
  return rc;
}
//-------------------------------------------------------------------------------------------------------------
static bool  mydb_check_key (IN sDB *db, IN const sDBT *key, IN const sDBT *data)
{
  /* empty keys are not supported: zero size is the iteration start marker */
  if ( !db || !key || !key->data || !key->size || (data && !data->data && data->size)
    || MYDB_ENTRY_OVERHEAD + key->size + (data ? data->size : 0U) > block_max_entry (db) )
  {
    mydb_errno = MYDB_ERR_KVSIZE;
    return false;
  }
  return true;
}
int  mydb_delete (IN sDB *db, IN const sDBT *key)
{
  if ( !mydb_check_key (db, key, NULL) )
    return -1;
  //-----------------------------------------
  /* Write ahead log */
  if ( mydb_logging (db, LOG_DELETE, key, NULL) )
    return -1;
  //-----------------------------------------
  int rc;
  do
  {
    if ( mydb_split_root (db) || !mydb_root (db) )
      return -1;
  } while ( (rc = block_delete_deep (db->root_, key)) > 0 );
  //-----------------------------------------
  /* an inner root without keys has a single child: the tree becomes lower */
  sBlock *rb;
  while ( (rb = mydb_root (db)) && *block_type (rb) == Pass && *block_nkvs (rb) == 0U )
  {
    db->head_.offset2root_ = rb->head_->pointer_0_;
    block_release (db, rb->offset_);
  }
  return rc;
}
int  mydb_insert (IN sDB *db, IN const sDBT *key, IN const sDBT *data)
{
  if ( !mydb_check_key (db, key, data) || !data )
    return -1;
  //-----------------------------------------
  /* Write ahead log */
  if ( mydb_logging (db, LOG_INSERT, key, data) )
    return -1;
  //-----------------------------------------
  if ( mydb_split_root (db) || !mydb_root (db) )
    return -1;
  return (DONE == block_add_nonfull (db->root_, key, data)) ? 0 : -1;
}
int  mydb_select (IN sDB *db, IN const sDBT *key, OUT      sDBT *data)
{
  if ( !mydb_check_key (db, key, NULL) || !data || !mydb_root (db) )
    return -1;
  return (DONE == block_select_deep (db->root_, key, data)) ? 0 : -1;
}
/* checkpoint: all pages to disk, then the journal is not needed anymore */
int  mydb_flush  (IN sDB *db)
{
  if ( mydb_sync_pages (db) || mydb_log_checkpoint (db) )
    return -1;
  return 0;
}
//-------------------------------------------------------------------------------------------------------------
