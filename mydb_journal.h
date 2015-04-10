#include "stdafx.h"
#include "mydb.h"

#ifndef _JOURNAL_H_
#define _JOURNAL_H_
//-------------------------------------------------------------------------------------------------------------
typedef struct Logging sLog;
struct Logging
{
  int i;

};
typedef struct LogRecord sLogRec;
struct LogRecord
{
  int j;

};
//-------------------------------------------------------------------------------------------------------------
/*   Открыть файл журнала на запись или чтение,
 *   и вернуть объект журнала.
 */
sLog*  log_open  (void);
void   log_close (sLog *log);

/* Записать в журнал log record. */
int       log_write (sLog *log, sLogRec *record);
/* Поместить позицию чтения на последний чекпоинт журнала */
eDBState  log_seek  (sLog *log);
/* Прочитать текущую запись из журнала
 * и сместить позицию чтения.
 */
sLogRec*  log_read_next ();
//-------------------------------------------------------------------------------------------------------------
#endif // _JOURNAL_H_
