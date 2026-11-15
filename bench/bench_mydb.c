/*  Throughput benchmark of mydb.
 *
 *  usage: bench_mydb [N] [file]
 *    N    - number of keys, 200000 by default
 *    file - database file, bench.db by default
 *
 *  For every configuration (page size, cache size) it measures: inserts in
 *  sequential and random order, lookups of present and absent keys, updates,
 *  deletes and the checkpoint. Keys are 16 bytes, values 32 bytes.
 *  The output is a Markdown table.
 */
#define _XOPEN_SOURCE 700
#include <time.h>

#include "mydb_common.h"
#include "mydb.h"

//-------------------------------------------------------------------------------------------------------------
#define KEY_SIZE 16U
#define VAL_SIZE 32U

static double now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
static void make_key(uchar_t *key, uint64_t i)
{
    /* big-endian counter: sequential numbers are sequential keys */
    memset(key, 'k', KEY_SIZE);
    for (int b = 0; b < 8; ++b)
        key[KEY_SIZE - 1 - b] = (uchar_t)(i >> (8 * b));
}
static uint64_t mix(uint64_t x)
{ /* splitmix64: a permutation of 64-bit numbers */
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}
static void fail(const char *what)
{
    fprintf(stderr, "bench: %s failed\n", what);
    exit(EXIT_FAILURE);
}
//-------------------------------------------------------------------------------------------------------------
typedef struct
{
    const char *name;
    double seconds;
    size_t ops;
} sResult;

static void report(const char *config, sResult *r, int n)
{
    for (int i = 0; i < n; ++i)
    {
        double us = r[i].seconds * 1e6 / (double)r[i].ops;
        printf("| %-22s | %-18s | %9zu | %10.0f | %8.2f |\n", config, r[i].name, r[i].ops,
               (double)r[i].ops / r[i].seconds, us);
    }
}
//-------------------------------------------------------------------------------------------------------------
static void run(const char *file, size_t n, size_t page_size, size_t cache_size)
{
    uchar_t key[KEY_SIZE], val[VAL_SIZE];
    memset(val, 'v', sizeof(val));

    sDBC conf = {0};
    conf.db_size = (size_t)1U << 30; /* sparse: only used pages take disk space */
    conf.page_size = page_size;
    conf.cache_size = cache_size;

    sResult r[8];
    int nr = 0;
    double t;
    //-----------------------------------------
    sDB *db = dbcreate(file, &conf);
    if (!db)
        fail("dbcreate");
    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        make_key(key, i);
        if (db_insert(db, key, KEY_SIZE, val, VAL_SIZE))
            fail("sequential insert");
    }
    r[nr++] = (sResult){"insert, sequential", now() - t, n};
    db_close(db);
    //-----------------------------------------
    if (!(db = dbcreate(file, &conf)))
        fail("dbcreate");
    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        make_key(key, mix(i));
        if (db_insert(db, key, KEY_SIZE, val, VAL_SIZE))
            fail("random insert");
    }
    r[nr++] = (sResult){"insert, random", now() - t, n};

    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        void *v;
        size_t len;
        make_key(key, mix((i * 7919U) % n));
        if (db_select(db, key, KEY_SIZE, &v, &len) || len != VAL_SIZE)
            fail("select");
        free(v);
    }
    r[nr++] = (sResult){"select, present", now() - t, n};

    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        void *v;
        size_t len;
        make_key(key, mix(n + i));
        if (!db_select(db, key, KEY_SIZE, &v, &len))
            fail("select of an absent key");
    }
    r[nr++] = (sResult){"select, absent", now() - t, n};

    val[0] = 'u';
    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        make_key(key, mix(i));
        if (db_insert(db, key, KEY_SIZE, val, VAL_SIZE))
            fail("update");
    }
    r[nr++] = (sResult){"update", now() - t, n};

    t = now();
    if (db_flush(db))
        fail("flush");
    r[nr++] = (sResult){"checkpoint", now() - t, 1};

    t = now();
    for (size_t i = 0; i < n; ++i)
    {
        make_key(key, mix(i));
        if (db_delete(db, key, KEY_SIZE))
            fail("delete");
    }
    r[nr++] = (sResult){"delete, random", now() - t, n};

    if (db_check(db) != 0)
        fail("db_check");
    db_close(db);
    //-----------------------------------------
    char config[64];
    snprintf(config, sizeof(config), "page %zuK, cache %zuM", page_size / 1024U, cache_size >> 20);
    report(config, r, nr);
    fflush(stdout);
}
//-------------------------------------------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    size_t n = (argc > 1) ? (size_t)strtoull(argv[1], NULL, 10) : 200000U;
    const char *file = (argc > 2) ? argv[2] : "bench.db";

    printf("%zu keys of %u bytes, values of %u bytes\n\n", n, KEY_SIZE, VAL_SIZE);
    printf("| configuration          | operation          |       ops |      ops/s | us/op    |\n");
    printf("|------------------------|--------------------|----------:|-----------:|---------:|\n");
    run(file, n, 4096U, 16U << 20); /* the whole tree in the cache */
    run(file, n, 4096U, 1U << 20);  /* the cache is smaller than the tree */
    run(file, n, 16384U, 16U << 20);

    char log[1024];
    snprintf(log, sizeof(log), "%s.log", file);
    unlink(file);
    unlink(log);
    return EXIT_SUCCESS;
}
//-------------------------------------------------------------------------------------------------------------
