/*  The worst and the awkward cases of the tree.
 *
 *  Insertion splits pages by bytes (at the edge for sequential keys), deletion merges
 *  and borrows when a page falls below a quarter (see src/mydb_block_delete.c).
 *  These tests run the workloads that hurt such a tree most, check that the tree stays
 *  correct and keeps its fill, and print its shape (db_stats) next to the shape of a
 *  "rebuilt" tree: the same keys inserted into a fresh database in a random order.
 *
 *  usage: test_worst [N]  (20000 keys by default); prints a Markdown table.
 */
#define _XOPEN_SOURCE 700
#include <time.h>

#include "stdafx.h"
#include "mydb.h"

//-------------------------------------------------------------------------------------------------------------
#define KEY_SIZE 9U
#define VAL_SIZE 16U
#define FILE_A   "w_tree.db"
#define FILE_B   "w_rebuilt.db"

static int n_failed;
#define CHECK(cond)                                                                                          \
    do                                                                                                       \
    {                                                                                                        \
        if (!(cond))                                                                                         \
        {                                                                                                    \
            ++n_failed;                                                                                      \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                         \
        }                                                                                                    \
    } while (0)

static double now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
static uint64_t mix(uint64_t x)
{ /* splitmix64: a permutation of 64-bit numbers */
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}
/* 'k' + big-endian counter: sequential numbers are sequential keys */
static void make_key(uchar_t *key, uint64_t i)
{
    key[0] = 'k';
    for (int b = 0; b < 8; ++b)
        key[KEY_SIZE - 1 - b] = (uchar_t)(i >> (8 * b));
}
//-------------------------------------------------------------------------------------------------------------
/* the set of keys in the tree (numbers), to rebuild and to look up */
typedef struct
{
    bool *present;
    size_t cap;
} sSet;

static sDB *open_db(const char *file, size_t page_size, size_t cache_size)
{
    sDBC conf = {(size_t)1U << 30, page_size, cache_size}; /* sparse: only used pages take disk space */
    sDB *db = dbcreate(file, &conf);
    if (!db)
    {
        fprintf(stderr, "dbcreate %s failed\n", file);
        exit(EXIT_FAILURE);
    }
    return db;
}
static void put(sDB *db, sSet *s, uint64_t id, size_t vsz)
{
    static uchar_t val[4096];
    uchar_t key[KEY_SIZE];
    make_key(key, id);
    memset(val, 'v', vsz);
    CHECK(db_insert(db, key, KEY_SIZE, val, vsz) == 0);
    if (s)
        s->present[id] = true;
}
static void del(sDB *db, sSet *s, uint64_t id)
{
    uchar_t key[KEY_SIZE];
    make_key(key, id);
    CHECK(db_delete(db, key, KEY_SIZE) == 0);
    if (s)
        s->present[id] = false;
}
/* average time of a lookup of every present key, in microseconds */
static double lookups_us(sDB *db, const sSet *s)
{
    /* the present keys in a random order; every key is looked up at least 3 times */
    size_t n = 0U, *ids = malloc(s->cap * sizeof(size_t));
    if (!ids)
        return 0.0;
    for (size_t i = 0U; i < s->cap; ++i)
    {
        size_t id = (size_t)(mix(i) % s->cap);
        if (s->present[id])
            ids[n++] = id;
    }
    size_t rounds = n ? 1U + (3U * s->cap) / n : 0U;
    uchar_t key[KEY_SIZE];
    double t = now();
    for (size_t r = 0U; r < rounds; ++r)
        for (size_t i = 0U; i < n; ++i)
        {
            void *v;
            size_t len;
            make_key(key, ids[i]);
            CHECK(db_select(db, key, KEY_SIZE, &v, &len) == 0);
            free(v);
        }
    t = now() - t;
    free(ids);
    return n ? t * 1e6 / (double)(n * rounds) : 0.0;
}
//-------------------------------------------------------------------------------------------------------------
typedef struct
{
    sDBStat st;
    double us;
} sShape;

static sShape shape(sDB *db, const sSet *s)
{
    sShape r;
    CHECK(db_stats(db, &r.st) == 0);
    /* the tree is valid, has exactly the expected keys, no page is lost */
    long keys = db_check(db);
    size_t expected = 0U;
    for (size_t i = 0U; i < s->cap; ++i)
        expected += s->present[i];
    CHECK(keys == (long)expected && r.st.keys == expected);
    CHECK(r.st.file_pages == r.st.leaf_pages + r.st.inner_pages);
    r.us = lookups_us(db, s);
    return r;
}
static void row(const char *name, const sShape *a, const sShape *b)
{
    printf("| %-34s | %7lu | %2u / %-2u | %6lu / %-6lu | %5lu | %3.0f%% / %3.0f%% | %5.2f / %-5.2f |\n", name,
           a->st.keys, a->st.height, b->st.height, a->st.leaf_pages + a->st.inner_pages,
           b->st.leaf_pages + b->st.inner_pages, a->st.empty_pages,
           a->st.capacity_bytes ? 100.0 * (double)a->st.used_bytes / (double)a->st.capacity_bytes : 0.0,
           b->st.capacity_bytes ? 100.0 * (double)b->st.used_bytes / (double)b->st.capacity_bytes : 0.0,
           a->us, b->us);
    fflush(stdout);
}
/* compare the tree with the same keys inserted into a fresh tree in a random order */
static void report(const char *name, sDB *db, const sSet *s, size_t page_size, size_t vsz)
{
    sShape a = shape(db, s);
    /* the deletion keeps the pages filled, sequential inserts fill them at the edge */
    double fill = a.st.capacity_bytes ? (double)a.st.used_bytes / (double)a.st.capacity_bytes : 1.0;
    if (strstr(name, "delete") || strstr(name, "keep"))
        CHECK(a.st.empty_pages == 0U && fill >= 0.20);
    if (strstr(name, "ascending") || strstr(name, "descending"))
        CHECK(fill >= 0.60);
    db_close(db);

    sDB *fresh = open_db(FILE_B, page_size, 16U << 20);
    for (size_t i = 0U; i < s->cap; ++i)
    {
        size_t id = (size_t)(mix(i ^ 0x5555U) % s->cap); /* a permutation is not needed: duplicates update */
        if (s->present[id])
            put(fresh, NULL, id, vsz);
    }
    for (size_t id = 0U; id < s->cap; ++id) /* the ids the hash above missed */
        if (s->present[id])
            put(fresh, NULL, id, vsz);
    sShape b = shape(fresh, s);
    db_close(fresh);
    row(name, &a, &b);
}
//-------------------------------------------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    size_t n = (argc > 1) ? (size_t)strtoull(argv[1], NULL, 10) : 20000U;
    const size_t page = 512U, cache = 16U << 20; /* small pages: more levels; the whole tree is cached */
    sSet s = {calloc(n, sizeof(bool)), n};
    if (!s.present)
        return EXIT_FAILURE;

    printf("%zu keys of %u bytes, values of %u bytes, pages of %zu bytes, cache %zu MB.\n", n, KEY_SIZE,
           VAL_SIZE, page, cache >> 20);
    printf("Each cell: this tree / the same keys rebuilt in a fresh tree.\n\n");
    printf(
        "| case                               |    keys | height  | pages           | empty | fill        | "
        "lookup, us    |\n");
    printf(
        "|------------------------------------|--------:|---------|-----------------|------:|-------------|"
        "---------------|\n");
    sDB *db;
    //-----------------------------------------
    /* baseline */
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < n; ++i)
        put(db, &s, mix(i) % n, VAL_SIZE);
    report("random inserts (baseline)", db, &s, page, VAL_SIZE);
    //-----------------------------------------
    /* splits by bytes leave the left half full and the right one is filled later: ascending order
     * leaves every left half at ~50% forever */
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < n; ++i)
        put(db, &s, i, VAL_SIZE);
    report("ascending inserts", db, &s, page, VAL_SIZE);

    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = n; i-- > 0U;)
        put(db, &s, i, VAL_SIZE);
    report("descending inserts", db, &s, page, VAL_SIZE);
    //-----------------------------------------
    /* mass deletes: pages are merged and borrow from their siblings */
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < n; ++i)
        put(db, &s, i, VAL_SIZE);
    for (size_t i = 0U; i < n; ++i)
        if (mix(i + 7U) % 10U)
            del(db, &s, i);
    report("delete 90% at random", db, &s, page, VAL_SIZE);

    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < n; ++i)
        put(db, &s, i, VAL_SIZE);
    for (size_t i = 0U; i < n; ++i)
        if (i % 100U)
            del(db, &s, i);
    report("keep every 100th key", db, &s, page, VAL_SIZE);

    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < n; ++i)
        put(db, &s, i, VAL_SIZE);
    for (size_t i = 0U; i + 10U < n; ++i)
        del(db, &s, i); /* all but the 10 last keys */
    report("delete all but 10 keys", db, &s, page, VAL_SIZE);
    //-----------------------------------------
    /* a queue: insert new keys at the right end, delete the oldest at the left */
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    size_t window = n / 20U;
    for (size_t i = 0U; i < n; ++i)
    {
        put(db, &s, i, VAL_SIZE);
        if (i >= window)
            del(db, &s, i - window);
    }
    report("sliding window (a queue)", db, &s, page, VAL_SIZE);
    //-----------------------------------------
    /* churn: random inserts and deletes around a half of the keys */
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    for (size_t i = 0U; i < 4U * n; ++i)
    {
        size_t id = (size_t)(mix(i * 31U) % n);
        if (s.present[id])
            del(db, &s, id);
        else
            put(db, &s, id, VAL_SIZE);
    }
    report("random insert/delete churn", db, &s, page, VAL_SIZE);
    //-----------------------------------------
    /* the largest entries: a few per page, a high tree */
    size_t big = n / 10U;
    memset(s.present, 0, n);
    db = open_db(FILE_A, page, cache);
    size_t vmax = db_max_kv_size(db) - KEY_SIZE;
    for (size_t i = 0U; i < big; ++i)
        put(db, &s, mix(i) % big, vmax);
    sShape a = shape(db, &s);
    db_close(db);
    sSet small = s; /* the same keys with small values */
    db = open_db(FILE_B, page, cache);
    for (size_t i = 0U; i < big; ++i)
        if (s.present[i])
            put(db, NULL, i, VAL_SIZE);
    sShape b = shape(db, &small);
    db_close(db);
    row("max-size values (vs 16-byte values)", &a, &b);
    //-----------------------------------------
    unlink(FILE_A);
    unlink(FILE_A ".log");
    unlink(FILE_B);
    unlink(FILE_B ".log");
    free(s.present);
    printf("\n%s\n", n_failed ? "FAILED" : "all worst cases passed");
    return n_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
//-------------------------------------------------------------------------------------------------------------
