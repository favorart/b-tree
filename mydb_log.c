#include "stdafx.h"
#include "mydb_low.h"
#include "mydb_log.h"
#include "mydb_log_low.h"
#include "mydb_error.h"

//-------------------------------------------------------------------------------------------------------------
static uint32_t  fnv1a (IN uint32_t hash, IN const void *data, IN size_t size)
{
  const uchar_t *p = (const uchar_t*) data;
  for ( size_t i = 0U; i < size; ++i )
  {
    hash ^= p[i];
    hash *= 16777619U;
  }
  return hash;
}
#define  FNV1A_INIT  2166136261U
//-------------------------------------------------------------------------------------------------------------
/* Открыть файл журнала на запись или чтение, и вернуть объект журнала */
sLog* mydb_log_open  (IN const char *db_file, IN bool trunc)
{
  static const char suffix[] = ".log";
  //-----------------------------------
  sLog* log = (sLog*) calloc (1U, sizeof (*log));
  if ( log )
    log->filename = (char*) malloc (strlen (db_file) + sizeof (suffix));
  if ( !log || !log->filename )
  {
    fprintf (stderr, "log open: %s\n", strerror (errno));
    free (log);
    return NULL;
  }
  strcpy (log->filename, db_file);
  strcat (log->filename, suffix);
  //-----------------------------------
  log->lsn = 0U;
  log->fd = open (log->filename, O_CREAT | ((trunc) ? O_TRUNC : 0) |
                                 O_RDWR  | O_APPEND | O_BINARY,
                                 S_IREAD | S_IWRITE);
  if ( log->fd == -1 )
  {
    fprintf (stderr, "log open '%s': %s\n", log->filename, strerror (errno));
    free (log->filename);
    free (log);
    return NULL;
  }
  //-----------------------------------
  return log;
}
void  mydb_log_close (IN sDB *db)
{ //-----------------------------------
  if ( db->log_ )
  { close (db->log_->fd);
    free  (db->log_->filename);
    free  (db->log_);
  }
  db->log_ = NULL;
}
//-------------------------------------------------------------------------------------------------------------
/* Записать в журнал log record */
bool  mydb_log_write (IN sLog *log, IN sLogRec *record)
{
  record->lsn = ++log->lsn;
  //-----------------------------------
  size_t  size = MYDB_LOG_HEAD_SIZE + record->key.size + record->val.size + sizeof (uint32_t);
  uchar_t *buf = (uchar_t*) malloc (size);
  if ( !buf )
    return true;

  mydb_wr32 (buf + 0U, (uint32_t) record->type);
  mydb_wr32 (buf + 4U, (uint32_t) record->key.size);
  mydb_wr32 (buf + 8U, (uint32_t) record->val.size);
  memcpy    (buf + 12U, &record->lsn, sizeof (record->lsn));
  if ( record->key.size )
    memcpy (buf + MYDB_LOG_HEAD_SIZE, record->key.data, record->key.size);
  if ( record->val.size )
    memcpy (buf + MYDB_LOG_HEAD_SIZE + record->key.size, record->val.data, record->val.size);
  mydb_wr32 (buf + size - sizeof (uint32_t), fnv1a (FNV1A_INIT, buf, size - sizeof (uint32_t)));
  //-----------------------------------
  /* one write () per record: the OS keeps it, even if the process crashes */
  bool fail = (write (log->fd, buf, size) != (ssize_t) size);
  free (buf);
  return fail;
}
//-------------------------------------------------------------------------------------------------------------
/* Прочитать текущую запись (malloced) из журнала и сместить позицию чтения */
sLogRec*  mydb_log_record_next (IN sLog    *log)
{
  uchar_t head[MYDB_LOG_HEAD_SIZE];
  if ( read (log->fd, head, sizeof (head)) != (ssize_t) sizeof (head) )
    return NULL; /* end of the journal or a torn record */

  uint32_t type = mydb_rd32 (head + 0U);
  uint32_t ksz  = mydb_rd32 (head + 4U);
  uint32_t vsz  = mydb_rd32 (head + 8U);
  if ( type > LOG_CHKPNT || ksz > (1U << 30) || vsz > (1U << 30) )
    return NULL; /* garbage */
  //-----------------------------------
  sLogRec *record = (sLogRec*) calloc (1U, sizeof (*record));
  size_t     size = (size_t) ksz + vsz + sizeof (uint32_t);
  if ( !record || !(record->buf = malloc (size)) )
  {
    fprintf (stderr, "log read next: %s\n", strerror (errno));
    free (record);
    return NULL;
  }
  if ( read (log->fd, record->buf, size) != (ssize_t) size )
  {
    mydb_log_record_free (record);
    return NULL;
  }
  //-----------------------------------
  uint32_t sum = fnv1a (fnv1a (FNV1A_INIT, head, sizeof (head)), record->buf, ksz + vsz);
  if ( sum != mydb_rd32 ((uchar_t*) record->buf + ksz + vsz) )
  {
    mydb_log_record_free (record);
    return NULL;
  }
  //-----------------------------------
  record->type     = (mydb_log_enum) type;
  memcpy (&record->lsn, head + 12U, sizeof (record->lsn));
  record->key.data = record->buf;
  record->key.size = ksz;
  record->val.data = (uchar_t*) record->buf + ksz;
  record->val.size = vsz;
  return record;
}
void      mydb_log_record_free (IN sLogRec *record)
{ //-----------------------------------
  if ( record )
  { free (record->buf);
    free (record);
  }
}
//-------------------------------------------------------------------------------------------------------------
int   mydb_logging (IN sDB *db, IN mydb_log_enum type, IN const sDBT *key, IN const sDBT *val)
{
  if ( !db->log_ || db->replaying_ )
    return 0;

  sLogRec record = { LOG_NONERC, 0U, { NULL, 0U }, { NULL, 0U }, NULL };
  //-----------------------------------
  if ( key ) record.key = *key;
  if ( val ) record.val = *val;
  record.type = type;
  //-----------------------------------
  if ( mydb_log_write (db->log_, &record) )
  {
    mydb_errno = MYDB_ERR_LOGWRT;
    fprintf (stderr, "mydb logging%s %s\n", strmyerror (mydb_errno), strerror (errno));
    return -1;
  }
  return 0;
}
int   mydb_log_checkpoint (IN sDB *db)
{
  if ( !db->log_ )
    return 0;
  //-----------------------------------
  /* the pages are already on disk (mydb_sync_pages): the history is not needed */
  if ( ftruncate (db->log_->fd, 0) )
  {
    fprintf (stderr, "log checkpoint: %s\n", strerror (errno));
    return -1;
  }
  bool replaying = db->replaying_;
  db->replaying_ = false;
  int rc = mydb_logging (db, LOG_CHKPNT, NULL, NULL);
  db->replaying_ = replaying;

  if ( rc || fsync (db->log_->fd) )
    return -1;
  return 0;
}
bool  mydb_recover (IN sDB *db)
{
  /*  При восстановлении после сбоя, все операции журнала после
   *  последнего checkpoint повторяются. На диске всегда согласованное
   *  дерево (см. mydb_cache_push), а insert/delete идемпотентны:
   *  повтор уже применённой операции не меняет результата.
   */
  sLog *log = db->log_;
  if ( !log )
    return true;
  //-----------------------------------
  /* pass 1: find the last checkpoint */
  off_t    replay_from = 0;
  uint64_t lsn = 0U;
  sLogRec *record;

  lseek (log->fd, 0, SEEK_SET);
  while ( (record = mydb_log_record_next (log)) )
  {
    lsn = record->lsn;
    if ( record->type == LOG_CHKPNT )
      replay_from = lseek (log->fd, 0, SEEK_CUR);
    mydb_log_record_free (record);
  }
  log->lsn = lsn;
  //-----------------------------------
  /* pass 2: replay */
  db->replaying_ = true;

  lseek (log->fd, replay_from, SEEK_SET);
  while ( (record = mydb_log_record_next (log)) )
  {
    switch ( record->type )
    {
      case LOG_INSERT: /* failed the first time (e.g. no free blocks) - fails again */
        mydb_insert (db, &record->key, &record->val);
        break;
      case LOG_DELETE: /* the key may be absent already */
        mydb_delete (db, &record->key);
        break;
      default:
        break;
    }
    mydb_log_record_free (record);
  }
  db->replaying_ = false;
  //-----------------------------------
  /* write the recovered state and cut a possibly torn tail of the journal */
  return mydb_flush (db) != 0;
}
//-------------------------------------------------------------------------------------------------------------
