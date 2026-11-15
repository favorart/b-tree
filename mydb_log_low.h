#include "stdafx.h"
#include "mydb_log.h"

#ifndef _MYDB_LOG_LOW_H_
#define _MYDB_LOG_LOW_H_
//-------------------------------------------------------------------------------------------------------------
/* The journal (Write Ahead Log) keeps the history of changes of the database blocks */
typedef struct Logging sLog;
struct Logging
{
    const char *filename;
    HFILE fd;
    //------------------
    uint64_t lsn;
    uint_t n_recover;
};
/*   Every journal record contains the record number (LSN) - a monotonically increasing
 *   record counter, the operation type (insert / replace / delete), the key-value pair
 *   or only the key (for delete).
 *   The journal also needs special records for the events: checkpoint,
 *   opening and closing of the database. The format of these records is defined separately.
 *
 *      +------+-------+-----+-----+-----+-----+----------+
 *      | type |  LSN  | ksz | vsz | key | val | LogRecSz | values
 *      +------+-------+-----+-----+-----+-----+----------+
 *      | char | int64 |int32|int32| ksz | vsz | uint32_t | sizes
 *      +------+-------+-----+-----+-----+-----+----------+
 */
//-------------------------------------------------------------------------------------------------------------
typedef struct LogRecord sLogRec;
struct LogRecord
{
    mydb_log_enum type; // type of operation
    uint64_t lsn;
    //------------------
    sDBT key, val;
    //------------------
    uint32_t size;
};
//-------------------------------------------------------------------------------------------------------------
/* Write a log record to the journal. */
bool mydb_log_write(IN sLog *log, IN sLogRec *record);
/* Move the read position to the last checkpoint of the journal */
bool mydb_log_seek(IN sLog *log);
//-------------------------------------------------------------------------------------------------------------
/* Read the current record from the journal and move the read position */
sLogRec *mydb_log_record_next(IN sLog *log);
void mydb_log_record_free(IN sLogRec *record);
//-------------------------------------------------------------------------------------------------------------
#ifdef _DEBUG_LOG
void mydb_log_print(IN sLog *log, sLogRec *record);
#endif // _DEBUG_LOG
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOG_LOW_H_
