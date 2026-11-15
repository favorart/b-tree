#include "mydb_common.h"
#include "mydb_low.h"
#include "mydb_log.h"
#include "mydb_log_low.h"
#include "mydb_error.h"
#include "mydb_trace.h"

//-------------------------------------------------------------------------------------------------------------
static uint32_t fnv1a(IN uint32_t hash, IN const void *data, IN size_t size)
{
    const uchar_t *p = (const uchar_t *)data;
    for (size_t i = 0U; i < size; ++i)
    {
        hash ^= p[i];
        hash *= 16777619U;
    }
    return hash;
}
#define FNV1A_INIT 2166136261U
//-------------------------------------------------------------------------------------------------------------
/* open the journal "<db_file>.log" for reading and appending */
sLog *mydb_log_open(IN const char *db_file, IN bool trunc)
{
    static const char suffix[] = ".log";
    //-----------------------------------
    sLog *log = (sLog *)calloc(1U, sizeof(*log));
    if (log)
        log->filename = (char *)malloc(strlen(db_file) + sizeof(suffix));
    if (!log || !log->filename)
    {
        fprintf(stderr, "log open: %s\n", strerror(errno));
        free(log);
        return NULL;
    }
    strcpy(log->filename, db_file);
    strcat(log->filename, suffix);
    //-----------------------------------
    log->lsn = 0U;
    log->fd = open(log->filename, O_CREAT | ((trunc) ? O_TRUNC : 0) | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (log->fd == -1)
    {
        fprintf(stderr, "log open '%s': %s\n", log->filename, strerror(errno));
        free(log->filename);
        free(log);
        return NULL;
    }
    //-----------------------------------
    return log;
}
void mydb_log_close(IN sDB *db)
{ //-----------------------------------
    if (db->log_)
    {
        close(db->log_->fd);
        free(db->log_->filename);
        free(db->log_);
    }
    db->log_ = NULL;
}
//-------------------------------------------------------------------------------------------------------------
/* append a record to the journal */
bool mydb_log_write(IN sLog *log, IN sLogRec *record)
{
    record->lsn = ++log->lsn;
    //-----------------------------------
    size_t size = MYDB_LOG_HEAD_SIZE + record->key.size + record->val.size + sizeof(uint32_t);
    uchar_t *buf = (uchar_t *)malloc(size);
    if (!buf)
        return true;

    mydb_wr32(buf + 0U, (uint32_t)record->type);
    mydb_wr32(buf + 4U, (uint32_t)record->key.size);
    mydb_wr32(buf + 8U, (uint32_t)record->val.size);
    memcpy(buf + 12U, &record->lsn, sizeof(record->lsn));
    if (record->key.size)
        memcpy(buf + MYDB_LOG_HEAD_SIZE, record->key.data, record->key.size);
    if (record->val.size)
        memcpy(buf + MYDB_LOG_HEAD_SIZE + record->key.size, record->val.data, record->val.size);
    mydb_wr32(buf + size - sizeof(uint32_t), fnv1a(FNV1A_INIT, buf, size - sizeof(uint32_t)));
    //-----------------------------------
    /* one write () per record: the OS keeps it, even if the process crashes */
    off_t end = lseek(log->fd, 0, SEEK_END);
    bool fail = (end < 0 || write(log->fd, buf, size) != (ssize_t)size);
    /* a record cut by a full disk would hide the next ones from the recovery: cut it off */
    if (fail && end >= 0 && ftruncate(log->fd, end))
        fprintf(stderr, "mydb journal: the cut record is not removed: %s\n", strerror(errno));
    free(buf);
    return fail;
}
//-------------------------------------------------------------------------------------------------------------
/* read the record (malloc'ed) at the current position and move on */
sLogRec *mydb_log_record_next(IN sLog *log)
{
    uchar_t head[MYDB_LOG_HEAD_SIZE];
    if (read(log->fd, head, sizeof(head)) != (ssize_t)sizeof(head))
        return NULL; /* end of the journal or a torn record */

    uint32_t type = mydb_rd32(head + 0U);
    uint32_t ksz = mydb_rd32(head + 4U);
    uint32_t vsz = mydb_rd32(head + 8U);
    if (type > LOG_ABORT || ksz > (1U << 30) || vsz > (1U << 30))
        return NULL; /* garbage */
    //-----------------------------------
    sLogRec *record = (sLogRec *)calloc(1U, sizeof(*record));
    size_t size = (size_t)ksz + vsz + sizeof(uint32_t);
    if (!record || !(record->buf = malloc(size)))
    {
        fprintf(stderr, "log read next: %s\n", strerror(errno));
        free(record);
        return NULL;
    }
    if (read(log->fd, record->buf, size) != (ssize_t)size)
    {
        mydb_log_record_free(record);
        return NULL;
    }
    //-----------------------------------
    uint32_t sum = fnv1a(fnv1a(FNV1A_INIT, head, sizeof(head)), record->buf, ksz + vsz);
    if (sum != mydb_rd32((uchar_t *)record->buf + ksz + vsz))
    {
        mydb_log_record_free(record);
        return NULL;
    }
    //-----------------------------------
    record->type = (mydb_log_enum)type;
    memcpy(&record->lsn, head + 12U, sizeof(record->lsn));
    record->key.data = record->buf;
    record->key.size = ksz;
    record->val.data = (uchar_t *)record->buf + ksz;
    record->val.size = vsz;
    return record;
}
void mydb_log_record_free(IN sLogRec *record)
{ //-----------------------------------
    if (record)
    {
        free(record->buf);
        free(record);
    }
}
//-------------------------------------------------------------------------------------------------------------
int mydb_logging(IN sDB *db, IN mydb_log_enum type, IN const sDBT *key, IN const sDBT *val)
{
    if (!db->log_ || db->replaying_)
        return 0;

    sLogRec record = {LOG_NONERC, 0U, {NULL, 0U}, {NULL, 0U}, NULL};
    //-----------------------------------
    if (key)
        record.key = *key;
    if (val)
        record.val = *val;
    record.type = type;
    //-----------------------------------
    if (TRACE_ON(MYDB_TRACE_PAGES))
    {
        char buf[MYDB_TRACE_KEYBUF];
        TRACE(MYDB_TRACE_PAGES, "journal: LSN %llu %s %s", (unsigned long long)db->log_->lsn + 1U,
              (type == LOG_INSERT)   ? "insert"
              : (type == LOG_DELETE) ? "delete"
              : (type == LOG_ABORT)  ? "abort"
                                     : "checkpoint",
              key ? mydb_trace_key(key->data, key->size, buf) : "");
    }
    if (mydb_log_write(db->log_, &record))
    {
        mydb_errno = MYDB_ERR_LOGWRT;
        fprintf(stderr, "mydb logging%s %s\n", strmyerror(mydb_errno), strerror(errno));
        return -1;
    }
    return 0;
}
int mydb_log_checkpoint(IN sDB *db)
{
    if (!db->log_)
        return 0;
    //-----------------------------------
    /* the pages are already on disk (mydb_sync_pages): the history is not needed */
    if (ftruncate(db->log_->fd, 0))
    {
        fprintf(stderr, "log checkpoint: %s\n", strerror(errno));
        return -1;
    }
    bool replaying = db->replaying_;
    db->replaying_ = false;
    int rc = mydb_logging(db, LOG_CHKPNT, NULL, NULL);
    db->replaying_ = replaying;

    if (rc || fsync(db->log_->fd))
        return -1;
    return 0;
}
bool mydb_recover(IN sDB *db)
{
    /*  After a crash all the operations after the last checkpoint are
   *  repeated. The disk always holds a consistent tree (see
   *  mydb_cache_push), and insert/delete are idempotent: repeating
   *  an already applied operation does not change the result.
   */
    sLog *log = db->log_;
    if (!log)
        return true;
    //-----------------------------------
    /* pass 1: find the last checkpoint */
    off_t replay_from = 0;
    uint64_t lsn = 0U;
    sLogRec *record;

    lseek(log->fd, 0, SEEK_SET);
    while ((record = mydb_log_record_next(log)))
    {
        lsn = record->lsn;
        if (record->type == LOG_CHKPNT)
            replay_from = lseek(log->fd, 0, SEEK_CUR);
        mydb_log_record_free(record);
    }
    log->lsn = lsn;
    //-----------------------------------
    /* pass 2: replay */
    db->replaying_ = true;
    unsigned long replayed = 0UL;
    TRACE(MYDB_TRACE_OPS, "recovery: the journal has %llu records, replay after the checkpoint at byte %lld",
          (unsigned long long)lsn, (long long)replay_from);

    /* an operation is applied when the next record is read: an ABORT right after it cancels it */
    lseek(log->fd, replay_from, SEEK_SET);
    sLogRec *pending = NULL;
    unsigned long aborted = 0UL;
    do
    {
        record = mydb_log_record_next(log);
        if (record && record->type == LOG_ABORT)
        {
            aborted += (pending != NULL);
            mydb_log_record_free(pending);
            mydb_log_record_free(record);
            pending = NULL;
            continue;
        }
        if (pending && pending->type == LOG_INSERT)
            mydb_insert(db, &pending->key, &pending->val);
        else if (pending && pending->type == LOG_DELETE) /* the key may be absent already */
            mydb_delete(db, &pending->key);
        replayed += (pending != NULL);
        mydb_log_record_free(pending);
        pending = NULL;
        if (record && (record->type == LOG_INSERT || record->type == LOG_DELETE))
            pending = record;
        else
            mydb_log_record_free(record);
    } while (record);
    db->replaying_ = false;
    TRACE(MYDB_TRACE_OPS, "recovery: %lu operations replayed, %lu failed ones skipped", replayed, aborted);
    //-----------------------------------
    /* write the recovered state and cut a possibly torn tail of the journal */
    return mydb_flush(db) != 0;
}
//-------------------------------------------------------------------------------------------------------------
off_t mydb_log_size(IN sDB *db)
{
    return db->log_ ? lseek(db->log_->fd, 0, SEEK_END) : -1;
}
//-------------------------------------------------------------------------------------------------------------
