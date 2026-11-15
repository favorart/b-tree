#include <stdarg.h>
#include <ctype.h>

#include "mydb_common.h"
#include "mydb_trace.h"

//-------------------------------------------------------------------------------------------------------------
int mydb_trace_level = MYDB_TRACE_OFF;

static FILE *trace_out = NULL;   /* NULL - stderr */
static bool trace_owned = false; /* trace_out was opened here */
static bool trace_set = false;   /* db_trace () or the environment was applied */
static unsigned long trace_opno = 0UL;
//-------------------------------------------------------------------------------------------------------------
static void trace_close_owned(void)
{
    if (trace_owned && trace_out)
        fclose(trace_out);
    trace_out = NULL;
    trace_owned = false;
}
void db_trace(int level, FILE *out)
{
    trace_close_owned();
    trace_out = out;
    mydb_trace_level = (level < MYDB_TRACE_OFF)    ? MYDB_TRACE_OFF
                       : (level > MYDB_TRACE_DUMP) ? MYDB_TRACE_DUMP
                                                   : level;
    trace_set = true;
}
void mydb_trace_init(void)
{
    if (trace_set)
        return;
    trace_set = true;
    //-----------------------------------------
    const char *level = getenv("MYDB_TRACE");
    if (!level || !*level)
        return;
    db_trace(atoi(level), NULL);

    const char *file = getenv("MYDB_TRACE_FILE");
    if (mydb_trace_level && file && *file)
    {
        FILE *f = fopen(file, "a");
        if (!f)
        {
            fprintf(stderr, "mydb trace '%s': %s\n", file, strerror(errno));
            return;
        }
        setvbuf(f, NULL, _IOLBF, 0); /* whole lines survive a crash of the process */
        trace_out = f;
        trace_owned = true;
    }
}
void mydb_trace_op(void)
{
    ++trace_opno;
}
//-------------------------------------------------------------------------------------------------------------
void mydb_trace_printf(IN int level, IN const char *fmt, ...)
{
    FILE *out = trace_out ? trace_out : stderr;
    /* the deeper the level, the bigger the indent: operations, tree, pages, dumps */
    fprintf(out, "mydb #%lu %*s", trace_opno, 2 * (level - 1), "");

    va_list args;
    va_start(args, fmt);
    vfprintf(out, fmt, args);
    va_end(args);

    fputc('\n', out);
}
const char *mydb_trace_key(IN const void *data, IN size_t size, OUT char buf[MYDB_TRACE_KEYBUF])
{
    const uchar_t *p = data;
    bool text = (size > 0U);
    for (size_t i = 0U; i < size && text; ++i)
        text = isprint(p[i]) && p[i] != '"';
    //-----------------------------------------
    size_t n = 0U, cap = MYDB_TRACE_KEYBUF - 8U; /* room for the '...' and the quotes */
    if (text)
    {
        buf[n++] = '"';
        for (size_t i = 0U; i < size && n < cap; ++i)
            buf[n++] = (char)p[i];
        buf[n++] = '"';
    }
    else
    {
        buf[n++] = '0';
        buf[n++] = 'x';
        for (size_t i = 0U; i < size && n + 2U <= cap; ++i)
            n += (size_t)snprintf(buf + n, 3U, "%02x", p[i]);
    }
    if ((text && n - 2U < size) || (!text && (n - 2U) / 2U < size))
    {
        memcpy(buf + n, "...", 3U);
        n += 3U;
    }
    buf[n] = '\0';
    return buf;
}
//-------------------------------------------------------------------------------------------------------------
