#include "stdafx.h"
#include "mydb_log.h"
#include "mydb_log_low.h"


#define LOG_FILENAME "mydb_log.dat"
//-------------------------------------------------------------------------------------------------------------
/* Открыть файл журнала на запись или чтение, и вернуть объект журнала */
sLog* mydb_log_open  (IN bool trunc)
{
  //-----------------------------------
  sLog* log = (sLog*) calloc (1U, sizeof (*log));
  if ( !log )
  { fprintf (stderr, "log open%s\n", strerror (errno)); }
  else
  { //-----------------------------------
    log->lsn = 0UL;
    log->filename = LOG_FILENAME;
    log->fd = open (LOG_FILENAME, O_CREAT | ((trunc) ? O_TRUNC : 0) |
                                  O_RDWR  | O_APPEND | O_BINARY,
                                  S_IREAD | S_IWRITE);
    if ( log->fd == -1 )
    { fprintf (stderr, "log open%s\n", strerror (errno));
      free (log);
      log = NULL;
    }
    else
    { //-----------------------------------
      /* set the lsn */
      mydb_log_seek (log);
    }
  }
  //-----------------------------------
  return log;
}
void  mydb_log_close (IN sDB *db)
{ //-----------------------------------
  if ( db->log_ )
  { close (db->log_->fd);
    free  (db->log_);
  }
  db->log_ = NULL;
}
//-------------------------------------------------------------------------------------------------------------
/* Записать в журнал log record */
bool  mydb_log_write (IN sLog *log, IN sLogRec *record)
{
  record->lsn = ++log->lsn;

#ifdef _DEBUG_LOG
  mydb_log_print (log, record);
#endif // _DEBUG_LOG
  //-----------------------------------
  write (log->fd, &record->type, sizeof (record->type)); // check write?
  write (log->fd, &record->lsn,  sizeof (record->lsn));  // check write?
  //-----------------------------------
  if ( record->type == LOG_INSERT || record->type == LOG_DELETE )
  {
     write (log->fd, &record->key.size, sizeof (record->key.size)); // check write?
    if ( record->type == LOG_INSERT )
     write (log->fd, &record->val.size, sizeof (record->val.size)); // check write?

     write (log->fd, record->key.data, sizeof (uchar_t) * record->key.size); // check write?
    if ( record->type == LOG_INSERT )
     write (log->fd, record->val.data, sizeof (uchar_t) * record->val.size); // check write?
  }
  write (log->fd, &record->size, sizeof (record->size)); // check write?
  //-----------------------------------
  return false;
}
/* Поместить позицию чтения на последний чекпоинт журнала */
bool  mydb_log_seek  (IN sLog *log)
{
  uint32_t rec_size = 0;
  uint32_t rec_nmbr = 0;
  uint32_t rec_type = LOG_NONERC;

  log->n_recover = 0U;
  log->lsn = 0UL;
  //-----------------------------------
  if ( 0 > lseek (log->fd, (0L - sizeof (rec_size) - rec_size), SEEK_END) )
    return false; // nothing here

  read (log->fd, &rec_size, sizeof (rec_size)); // check read?
  //-----------------------------------
  if ( 0 > lseek (log->fd, (0L - sizeof (rec_size) - rec_size), SEEK_CUR) )
  { /* если внезапно файл закончится */
    log->n_recover = 1U;
    lseek (log->fd, 0L, SEEK_SET); // check?
    return true;
  }
  read (log->fd, &rec_size, sizeof (rec_size)); // check read?
  read (log->fd, &rec_type, sizeof (rec_type)); // check read?

  /* set the global lsn */
  read (log->fd, &log->lsn, sizeof (log->lsn)); // check read?
  //-----------------------------------
  if ( rec_type != LOG_DB_CLS )
  {
    lseek (log->fd, (0L - sizeof (log->lsn)), SEEK_CUR); // ok
    while ( rec_type != LOG_CHKPNT )
    {
      ++log->n_recover;
      if ( 0 > lseek (log->fd, (0L - sizeof (rec_size) - sizeof (rec_type) - rec_size), SEEK_CUR) )
      { /* если внезапно файл закончится */
        ++log->n_recover;
        lseek (log->fd, 0L, SEEK_SET); // check?
        return true;
      }

      read (log->fd, &rec_size, sizeof (rec_size)); // check read?
      read (log->fd, &rec_type, sizeof (rec_type)); // check read?
    }

    ++log->n_recover;
    lseek (log->fd, (0L - sizeof (rec_type)), SEEK_CUR); // check? - must be ok
    return true;
  }
  else
  {
    lseek (log->fd, 0L, SEEK_END); // check? - must be ok
    return false;
  }
  //-----------------------------------
}
//-------------------------------------------------------------------------------------------------------------
/* Прочитать текущую запись (malloced) из журнала и сместить позицию чтения */
sLogRec*  mydb_log_record_next (IN sLog    *log)
{
  sLogRec* record = (sLogRec*) calloc (1U, sizeof (*record));
  if ( !record )
  { fprintf (stderr, "log read next%s\n", strerror (errno)); }
  else
  {
    if ( 0 >= read (log->fd, &record->type, sizeof (record->type)) )
    { /* stop reading */
      mydb_log_record_free (record);
      return (record = NULL);
    }
    read (log->fd, &record->lsn,  sizeof (record->lsn));  // check read?
    //-----------------------------------
    if ( record->type == LOG_INSERT || record->type == LOG_DELETE )
    {
        read (log->fd, &record->key.size, sizeof (record->key.size)); // check read?
      if ( record->type == LOG_INSERT )
        read (log->fd, &record->val.size, sizeof (record->val.size)); // check read?
      //-----------------------------------
      if ( !(record->key.data = malloc (sizeof (uchar_t) * (record->key.size + record->val.size))) )
      {
        fprintf (stderr, "log read next%s\n", strerror (errno));
        mydb_log_record_free (record);
        record = NULL;
      }
      else
      {
        if ( record->type == LOG_INSERT )
          record->val.data = ((uchar_t*) record->key.data + record->key.size);

        read (log->fd, record->key.data, sizeof (uchar_t) * (record->key.size + record->val.size)); // check read?
      }
    }
    //-----------------------------------    
  }

  if ( record )
    read (log->fd, &record->size, sizeof (record->size)); // check read?
  //-----------------------------------
  return record;
}
void      mydb_log_record_free (IN sLogRec *record)
{ //-----------------------------------
  if ( record )
  { free (record->key.data);
    free (record);
  }
}
//-------------------------------------------------------------------------------------------------------------
void  mydb_logging (IN sDB *db, IN mydb_log_enum type, IN const sDBT *key, IN const sDBT *val)
{
  sLogRec record = {};
  //-----------------------------------
  if ( key ) record.key = *key;
  if ( val ) record.val = *val;
  //-----------------------------------
  record.type = type;
  record.size = sizeof (record.type) + sizeof (record.lsn) + sizeof (record.size);

  if ( type == LOG_INSERT || type == LOG_DELETE )
    record.size += sizeof (record.key.size) + record.key.size;
  if ( type == LOG_INSERT )
    record.size += sizeof (record.val.size) + record.val.size;
  //-----------------------------------
  mydb_log_write (db->log_, &record);
}
bool  mydb_recover (IN sDB *db)
{
  bool was_fail = false;
  //-----------------------------------
  /*  При восстановлении после сбоя, производится
   *  сверка всех записей журнала после checkpoint
   *  с соответствующими страницами на диске:
   *  идея в том, что операция выполняется повторно,
   *  если она ещё не отражена в странице дерева.
   */
  if ( !db->log_ )
  { db->log_ = mydb_log_open (false); }

#ifdef _DEBUG_LOG
  printf ("\n\nrecovery:\n");
#endif // _DEBUG_LOG
  //-----------------------------------
  if ( db->log_ )
  {
    while ( db->log_->n_recover-- )
    {
      sLogRec *record = mydb_log_record_next (db->log_);
      if ( record )
      {
        switch ( record->type )
        {
          default:
            break;
          case LOG_INSERT: // was_fail |= (db->insert (db, record->key, record->val) != 0);
            break;
          case LOG_DELETE: // was_fail |= (db->delete (db, record->key) != 0);
            break;
          case LOG_CHKPNT: case LOG_DB_CLS: case LOG_DB_OPN: break;
        }

#ifdef _DEBUG_LOG
        mydb_log_print (db->log_, record);
#endif // _DEBUG_LOG

        mydb_log_record_free (record);
      }
      else break;
    }
  }
  else was_fail = true;
  //-----------------------------------
  return was_fail;
}
//-------------------------------------------------------------------------------------------------------------
#ifdef _DEBUG_LOG
void  mydb_log_print (IN sLog *log, sLogRec *record)
{
  const char *types[] = { "NONERC", "INSERT", "DELETE", "REMOVE", "DB_OPN", "DB_CLS", "CHKPNT" };
  //-----------------------------------
  // sLogRec *record;
  // while ( (record = mydb_log_record_next (log)) )
  {
    //-----------------------------------
    switch ( record->type )
    {
      case LOG_INSERT:
        printf ("%s lsn=%lld ksz=%d vsz=%d k=%d v=%d sz=%d\n",
                types[record->type], record->lsn,
                record->key.size, record->val.size,
                *(int*) record->key.data,
                *(int*) record->val.data, record->size);
        break;

      case LOG_DELETE:
        printf ("%s lsn=%lld ksz=%d k=%d sz=%d\n",
                types[record->type], record->lsn, record->key.size,
                *(int*) record->key.data, record->size);
        break;

      default:
        printf ("%s lsn=%lld sz=%d\n",
                types[record->type], record->lsn, record->size);
        break;
    }
    //-----------------------------------
    // mydb_log_record_free (record);
  }
  //-----------------------------------
  // printf ("\n");
  return;
}
#endif // _DEBUG_LOG
//-------------------------------------------------------------------------------------------------------------
