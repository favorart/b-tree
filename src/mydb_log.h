#include "mydb_common.h"
#include "mydb.h"

#ifndef _MYDB_LOG_H_
#define _MYDB_LOG_H_
//-------------------------------------------------------------------------------------------------------------
typedef enum
{
    LOG_NONERC,
    LOG_INSERT,
    LOG_DELETE,
    LOG_REMOVE,
    LOG_DB_OPN,
    LOG_DB_CLS,
    LOG_CHKPNT,
    LOG_ABORT /* the operation of the previous record failed: recovery skips it */
} mydb_log_enum;
//-------------------------------------------------------------------------------------------------------------
/* open the journal "<db_file>.log" for reading and appending */
sLog *mydb_log_open(IN const char *db_file, IN bool trunc);
void mydb_log_close(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
/* Append a record before the operation is applied (write-ahead); LOG_ABORT after a failed one. 0 on success */
int mydb_logging(IN sDB *db, IN mydb_log_enum type, IN const sDBT *key, IN const sDBT *val);
/* All pages are on disk: drop the journal and start it with a checkpoint record. 0 on success */
int mydb_log_checkpoint(IN sDB *db);
/* Replay the operations after the last checkpoint. true on error */
bool mydb_recover(IN sDB *db);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOG_H_
