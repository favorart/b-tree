#include "stdafx.h"
#include "mydb_log.h"

#ifndef _MYDB_LOG_LOW_H_
#define _MYDB_LOG_LOW_H_
//-------------------------------------------------------------------------------------------------------------
/* Журнал (Write Ahead Log) содержит историю операций над базой данных */
struct Logging
{
  char        *filename;
  HFILE        fd;
  //------------------
  uint64_t     lsn;
};
/*   Каждая запись в журнале содержит № записи (LSN) — монотонно увеличивающийся
 *   счётчик записи, тип операции (insert / delete / checkpoint), пару ключ-значение
 *   либо только ключ (для delete). Все числа little-endian (как в памяти x86).
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
#define  MYDB_LOG_HEAD_SIZE  20U
//-------------------------------------------------------------------------------------------------------------
typedef struct LogRecord sLogRec;
struct LogRecord
{
  mydb_log_enum  type;  // type of operation
  uint64_t        lsn;
  //------------------
  sDBT       key, val;
  void           *buf;  // malloc'ed storage of key and val (on read)
};
//-------------------------------------------------------------------------------------------------------------
/* Записать в журнал log record. true on error */
bool      mydb_log_write (IN sLog *log, IN sLogRec *record);
//-------------------------------------------------------------------------------------------------------------
/* Прочитать текущую запись из журнала и сместить позицию чтения. NULL at the end or at a torn record */
sLogRec*  mydb_log_record_next (IN sLog    *log);
void      mydb_log_record_free (IN sLogRec *record);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_LOG_LOW_H_
