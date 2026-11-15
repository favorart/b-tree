#include "stdafx.h"
#include "mydb_log.h"

#ifndef _MYDB_LOG_LOW_H_
#define _MYDB_LOG_LOW_H_
//-------------------------------------------------------------------------------------------------------------
/* The journal (write-ahead log) keeps the history of operations */
struct Logging
{
    char *filename;
    HFILE fd;
    //------------------
    uint64_t lsn;
};
/*   Every record has a log sequence number (LSN, a monotonically increasing
 *   counter), the type of the operation (insert / delete / checkpoint) and the
 *   key-value pair (only the key for delete). Numbers are in the native byte order.
 *
 *      +------+-----+-----+-------+-----+-----+----------+
 *      | type | ksz | vsz |  LSN  | key | val | checksum | values
 *      +------+-----+-----+-------+-----+-----+----------+
 *      |  u32 | u32 | u32 |  u64  | ksz | vsz |   u32    | sizes
 *      +------+-----+-----+-------+-----+-----+----------+
 *
 *   checksum is FNV-1a of all the previous bytes of the record:
 *   a torn record at the tail (crash during write) is detected and ignored.
 */
#define MYDB_LOG_HEAD_SIZE 20U
//-------------------------------------------------------------------------------------------------------------
typedef struct LogRecord sLogRec;
struct LogRecord
{
    mydb_log_enum type; // type of operation
    uint64_t lsn;
    //------------------
    sDBT key, val;
    void *buf; // malloc'ed storage of key and val (on read)
};
//-------------------------------------------------------------------------------------------------------------
/* append a record to the journal. true on error */
bool mydb_log_write(IN sLog *log, IN sLogRec *record);
//-------------------------------------------------------------------------------------------------------------
/* read the record at the current position and move on. NULL at the end or at a torn record */
sLogRec *mydb_log_record_next(IN sLog *log);
void mydb_log_record_free(IN sLogRec *record);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOG_LOW_H_
