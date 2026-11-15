/*  Tests for mydb: every operation is checked against a simple in-memory model
 *  (a sorted array). Run from the tests/ directory: it creates t_*.db files.
 */
#define _XOPEN_SOURCE 700
#include <signal.h>
#include <sys/wait.h>
#include <sys/resource.h>

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>

#include "mydb_common.h"
#include "mydb.h"

//-------------------------------------------------------------------------------------------------------------
/* cmocka in the test process; a child process of a crash test exits with 3 instead */
static bool in_child;
#define CHECK(cond)                                                                                          \
    do                                                                                                       \
    {                                                                                                        \
        if (in_child)                                                                                        \
        {                                                                                                    \
            if (!(cond))                                                                                     \
                _exit(3);                                                                                    \
        }                                                                                                    \
        else                                                                                                 \
            assert_true(cond);                                                                               \
    } while (0)

static uint64_t rng_state;
static uint64_t rng(void)
{ /* xorshift64* */
    rng_state ^= rng_state >> 12;
    rng_state ^= rng_state << 25;
    rng_state ^= rng_state >> 27;
    return rng_state * 2685821657736338717ULL;
}
static size_t rnd(size_t n)
{
    return (size_t)(rng() % n);
}
//-------------------------------------------------------------------------------------------------------------
/* The model: a sorted array of key-value pairs */
#define MAX_KEY 24U
typedef struct
{
    uchar_t k[MAX_KEY];
    size_t klen;
    uchar_t *v;
    size_t vlen;
} sItem;
typedef struct
{
    sItem *items;
    size_t n, cap;
} sModel;

static int item_cmp(const uchar_t *a, size_t alen, const uchar_t *b, size_t blen)
{
    int r = memcmp(a, b, alen < blen ? alen : blen);
    return r ? r : (alen < blen) ? -1 : (alen > blen);
}
static size_t model_find(sModel *m, const uchar_t *k, size_t klen, bool *found)
{
    size_t lo = 0U, hi = m->n;
    while (lo < hi)
    {
        size_t mid = (lo + hi) / 2U;
        int c = item_cmp(m->items[mid].k, m->items[mid].klen, k, klen);
        if (!c)
        {
            *found = true;
            return mid;
        }
        if (c < 0)
            lo = mid + 1U;
        else
            hi = mid;
    }
    *found = false;
    return lo;
}
static void model_put(sModel *m, const uchar_t *k, size_t klen, const uchar_t *v, size_t vlen)
{
    bool found;
    size_t i = model_find(m, k, klen, &found);
    if (!found)
    {
        if (m->n == m->cap)
        {
            m->cap = m->cap ? 2U * m->cap : 1024U;
            CHECK((m->items = realloc(m->items, m->cap * sizeof(sItem))));
        }
        memmove(&m->items[i + 1U], &m->items[i], (m->n - i) * sizeof(sItem));
        ++m->n;
        memcpy(m->items[i].k, k, klen);
        m->items[i].klen = klen;
        m->items[i].v = NULL;
    }
    free(m->items[i].v);
    CHECK((m->items[i].v = malloc(vlen + 1U)));
    memcpy(m->items[i].v, v, vlen);
    m->items[i].vlen = vlen;
}
static bool model_del(sModel *m, const uchar_t *k, size_t klen)
{
    bool found;
    size_t i = model_find(m, k, klen, &found);
    if (found)
    {
        free(m->items[i].v);
        memmove(&m->items[i], &m->items[i + 1U], (m->n - i - 1U) * sizeof(sItem));
        --m->n;
    }
    return found;
}
static void model_free(sModel *m)
{
    for (size_t i = 0U; i < m->n; ++i)
        free(m->items[i].v);
    free(m->items);
    memset(m, 0, sizeof(*m));
}
//-------------------------------------------------------------------------------------------------------------
static void check_value(sDB *db, const uchar_t *k, size_t klen, const uchar_t *v, size_t vlen)
{
    void *got = NULL;
    size_t len = 0U;
    CHECK(db_select(db, (void *)k, klen, &got, &len) == 0);
    CHECK(len == vlen);
    CHECK(memcmp(got, v, vlen) == 0);
    free(got);
}
static void expect_absent(sDB *db, const uchar_t *k, size_t klen)
{
    void *got = NULL;
    size_t len = 0U;
    CHECK(db_select(db, (void *)k, klen, &got, &len) != 0);
    CHECK(got == NULL);
}
static void verify_all(sDB *db, sModel *m)
{
    CHECK(db_check(db) == (long)m->n);
    for (size_t i = 0U; i < m->n; ++i)
        check_value(db, m->items[i].k, m->items[i].klen, m->items[i].v, m->items[i].vlen);
}
//-------------------------------------------------------------------------------------------------------------
/* A random operation, applied both to the db (if any) and to the model */
static uchar_t val_buf[1U << 16];
static void random_op(sDB *db, sModel *m, size_t max_kv)
{
    uchar_t k[MAX_KEY];
    size_t klen;

    if (m->n && rnd(100U) < 40U)
    { /* an existing key */
        sItem *it = &m->items[rnd(m->n)];
        klen = it->klen;
        memcpy(k, it->k, klen);
    }
    else
    {
        klen = 1U + rnd(MAX_KEY);
        for (size_t i = 0U; i < klen; ++i)
            k[i] = (uchar_t)('a' + rnd(4U));
    }

    if (rnd(100U) < 30U)
    {
        bool had = model_del(m, k, klen);
        if (db)
            CHECK((db_delete(db, k, klen) == 0) == had);
    }
    else
    {
        size_t vmax = max_kv - klen;
        size_t vlen = (rnd(8U) == 0U) ? vmax - rnd(8U) : rnd(vmax / 4U + 1U);
        for (size_t i = 0U; i < vlen; ++i)
            val_buf[i] = (uchar_t)rng();
        model_put(m, k, klen, val_buf, vlen);
        if (db)
            CHECK(db_insert(db, k, klen, val_buf, vlen) == 0);
    }
}
//-------------------------------------------------------------------------------------------------------------
static sDBC small_conf(void)
{
    sDBC conf = {0};
    conf.db_size = 8U * 1024U * 1024U;
    conf.page_size = 512U;
    conf.cache_size = 0U; /* default */
    return conf;
}
static sDBC tiny_cache_conf(void)
{
    sDBC conf = small_conf();
    conf.cache_size = 1U; /* clamped to MYDB_MIN_CACHE_PAGES: lots of evictions */
    return conf;
}
//-------------------------------------------------------------------------------------------------------------
static void test_basic(void **state)
{
    (void)state;
    sDBC conf = small_conf();
    sDB *db = dbcreate("t_basic.db", &conf);
    CHECK(db);

    CHECK(db_insert(db, "key", 3U, "value", 5U) == 0);
    check_value(db, (const uchar_t *)"key", 3U, (const uchar_t *)"value", 5U);
    CHECK(db_insert(db, "key", 3U, "longer value", 12U) == 0);
    check_value(db, (const uchar_t *)"key", 3U, (const uchar_t *)"longer value", 12U);
    CHECK(db_insert(db, "k", 1U, "", 0U) == 0);
    check_value(db, (const uchar_t *)"k", 1U, (const uchar_t *)"", 0U);
    expect_absent(db, (const uchar_t *)"ke", 2U);
    expect_absent(db, (const uchar_t *)"key2", 4U);

    /* invalid arguments */
    CHECK(db_insert(db, "", 0U, "v", 1U) != 0);
    CHECK(db_insert(db, "big", 3U, val_buf, db_max_kv_size(db)) != 0);
    CHECK(db_insert(db, "fit", 3U, val_buf, db_max_kv_size(db) - 3U) == 0);

    CHECK(db_delete(db, "key", 3U) == 0);
    CHECK(db_delete(db, "key", 3U) != 0);
    expect_absent(db, (const uchar_t *)"key", 3U);
    CHECK(db_close(db) == 0);

    /* an empty db survives reopen; a foreign file is rejected */
    CHECK((db = dbcreate("t_empty.db", &conf)));
    CHECK(db_close(db) == 0);
    CHECK((db = dbopen("t_empty.db")));
    expect_absent(db, (const uchar_t *)"x", 1U);
    CHECK(db_close(db) == 0);
    CHECK(dbopen("t_missing.db") == NULL);
    FILE *f = fopen("t_foreign.db", "wb");
    CHECK(f && fputs("definitely not a database, but long enough to have a header", f) >= 0);
    fclose(f);
    CHECK(dbopen("t_foreign.db") == NULL);
    printf("basic: ok\n");
}
//-------------------------------------------------------------------------------------------------------------
static void test_random(const char *name, sDBC conf, size_t n_ops, size_t reopen_every)
{
    sModel m = {0};
    sDB *db = dbcreate(name, &conf);
    CHECK(db);
    size_t max_kv = db_max_kv_size(db);

    for (size_t op = 1U; op <= n_ops; ++op)
    {
        random_op(db, &m, max_kv);
        if (op % 997U == 0U)
            verify_all(db, &m);
        if (reopen_every && op % reopen_every == 0U)
        {
            CHECK(db_close(db) == 0);
            CHECK((db = dbopen(name)));
            verify_all(db, &m);
        }
    }
    verify_all(db, &m);
    printf("random %-16s: ok (%zu ops, %zu keys)\n", name, n_ops, m.n);
    CHECK(db_close(db) == 0);
    model_free(&m);
}
//-------------------------------------------------------------------------------------------------------------
/* insert many keys, delete all of them, insert again: blocks are reused */
static void test_delete_all(void **state)
{
    (void)state;
    sDBC conf = tiny_cache_conf();
    sDB *db = dbcreate("t_delall.db", &conf);
    CHECK(db);

    enum
    {
        N = 6000
    };
    static uint32_t keys[N];
    for (int round = 0; round < 3; ++round)
    {
        for (uint32_t i = 0U; i < N; ++i)
        {
            keys[i] = i * 2654435761U;
            CHECK(db_insert(db, &keys[i], sizeof(keys[i]), &i, sizeof(i)) == 0);
        }
        for (uint32_t i = N - 1U; i > 0U; --i) /* shuffle */
        {
            uint32_t j = (uint32_t)rnd(i + 1U), t = keys[i];
            keys[i] = keys[j];
            keys[j] = t;
        }
        for (uint32_t i = 0U; i < N; ++i)
        {
            CHECK(db_delete(db, &keys[i], sizeof(keys[i])) == 0);
            if (i % 500U == 0U)
            {
                expect_absent(db, (const uchar_t *)&keys[i], sizeof(keys[i]));
                CHECK(db_check(db) == (long)(N - 1U - i));
            }
        }
        CHECK(db_check(db) == 0);
        for (uint32_t i = 0U; i < N; ++i)
            expect_absent(db, (const uchar_t *)&keys[i], sizeof(keys[i]));
    }
    CHECK(db_close(db) == 0);
    printf("delete all: ok\n");
}
//-------------------------------------------------------------------------------------------------------------
/* fill the file completely: inserts fail cleanly, the data stays */
static void test_full(void **state)
{
    (void)state;
    sDBC conf = small_conf();
    conf.db_size = 64U * 1024U;
    sDB *db = dbcreate("t_full.db", &conf);
    CHECK(db);

    sModel m = {0};
    uchar_t v[200];
    memset(v, 'v', sizeof(v));
    for (uint32_t i = 0U;; ++i)
    {
        uint32_t k = i * 2654435761U;
        if (db_insert(db, &k, sizeof(k), v, 40U) != 0)
            break;
        model_put(&m, (uchar_t *)&k, sizeof(k), v, 40U);
        CHECK(i < 100000U);
    }
    verify_all(db, &m);
    CHECK(db_close(db) == 0);
    CHECK((db = dbopen("t_full.db")));
    verify_all(db, &m);
    /* a full file: every delete succeeds and frees the space, no page is lost */
    size_t fit = m.n;
    for (uint32_t i = 0U; i < fit; ++i)
    {
        uint32_t k = i * 2654435761U;
        CHECK(db_delete(db, &k, sizeof(k)) == 0);
    }
    sDBStat st;
    CHECK(db_check(db) == 0 && db_stats(db, &st) == 0);
    CHECK(st.file_pages == st.leaf_pages + st.inner_pages && st.file_pages <= 2U);
    for (uint32_t i = 0U; i < fit; ++i)
    {
        uint32_t k = i * 2654435761U;
        CHECK(db_insert(db, &k, sizeof(k), v, 40U) == 0); /* the same keys fit again */
    }
    verify_all(db, &m);
    CHECK(db_close(db) == 0);
    printf("full file: ok (%zu keys fit, deleted and inserted again)\n", m.n);
    model_free(&m);
}
//-------------------------------------------------------------------------------------------------------------
/*  Crash recovery: a child process makes operations and dies without closing
 *  the db; the parent repeats the same operations on the model, reopens the
 *  db and compares. A tiny cache makes the child write pages in between.
 */
static void test_crash(int round, bool torn_tail)
{
    const char *name = "t_crash.db";
    sDBC conf = tiny_cache_conf();
    uint64_t seed = 0x5EED0000ULL + (uint64_t)round;
    size_t n_ops = 2000U + 1500U * (size_t)round;
    size_t flush_at = (round % 2) ? n_ops / 3U : 0U; /* a checkpoint in the middle */

    sDB *db = dbcreate(name, &conf);
    CHECK(db);
    size_t max_kv = db_max_kv_size(db);
    CHECK(db_close(db) == 0);

    fflush(NULL);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0)
    {
        in_child = true;
        sModel cm = {0};
        rng_state = seed;
        if (!(db = dbopen(name)))
            _exit(2);
        for (size_t op = 1U; op <= n_ops; ++op)
        {
            random_op(db, &cm, max_kv);
            if (op == flush_at)
                db_flush(db);
        }
        _exit(0); /* crash: no db_close (), dirty pages are lost */
    }
    int status = 0;
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    if (torn_tail)
    { /* a record half-written at the moment of the crash */
        FILE *f = fopen("t_crash.db.log", "ab");
        CHECK(f);
        fwrite("\x01\x00\x00\x00\x05\x00\x00\x00garbage", 1U, 15U, f);
        fclose(f);
    }

    sModel m = {0};
    uint64_t saved = rng_state;
    rng_state = seed;
    for (size_t op = 1U; op <= n_ops; ++op)
        random_op(NULL, &m, max_kv);
    rng_state = saved;

    CHECK((db = dbopen(name)));
    verify_all(db, &m);
    /* and once more after a clean close */
    CHECK(db_close(db) == 0);
    CHECK((db = dbopen(name)));
    verify_all(db, &m);
    CHECK(db_close(db) == 0);
    printf("crash recovery #%d%s: ok (%zu ops, %zu keys)\n", round, torn_tail ? " (torn tail)" : "", n_ops,
           m.n);
    model_free(&m);
}
/* a header that mydb_create () could not have made is rejected without touching the file */
/* a full file and a cache smaller than the tree: deletes split nothing when there is no free page */
static void test_full_delete_some(void **state)
{
    (void)state;
    sDBC conf = tiny_cache_conf();
    conf.db_size = 400U * 512U;
    sDB *db = dbcreate("t_full2.db", &conf);
    CHECK(db);
    uchar_t v[40];
    memset(v, 'v', sizeof(v));
    uint32_t n = 0U;
    while (db_insert(db, &n, sizeof(n), v, sizeof(v)) == 0)
        CHECK(++n < 100000U);
    for (uint32_t k = 0U; k < n; k += 3U)
        CHECK(db_delete(db, &k, sizeof(k)) == 0);
    CHECK(db_check(db) == (long)(n - (n + 2U) / 3U));
    sDBStat st;
    CHECK(db_stats(db, &st) == 0 && st.file_pages == st.leaf_pages + st.inner_pages);
    CHECK(db_close(db) == 0);
    printf("full file, deletes with a small cache: ok (%u keys fit)\n", n);
}
//-------------------------------------------------------------------------------------------------------------
static void test_open_bad_header(void **state)
{
    (void)state;
    sDBC conf = small_conf();
    sDB *db = dbcreate("t_header.db", &conf);
    CHECK(db);
    CHECK(db_close(db) == 0);
    sDBFH good;
    FILE *f = fopen("t_header.db", "rb");
    CHECK(f && fread(&good, sizeof(good), 1U, f) == 1U);
    fclose(f);

    for (int c = 0; c < 9; ++c)
    {
        sDBFH h = good;
        long truncate_to = -1;
        switch (c)
        {
        case 0:
            h.page_size_ = 1U << 25;
            h.cache_size_ = 0U;
            h.techb_count_ = 0U;
            h.block_count_ = 1U;
            break;
        case 1:
            h.page_size_ = 256U;
            break;
        case 2:
            h.techb_count_ = 0U;
            break;
        case 3:
            h.block_count_ += 100U;
            break;
        case 4:
            h.nodes_count_ = h.block_count_ + 1U;
            break;
        case 5:
            h.offset2root_ = h.block_count_;
            break;
        case 6:
            h.db_size_ = 100U;
            break;
        case 7:
            h.version_ = MYDB_VERSION + 1U;
            break;
        default:
            truncate_to = (long)sizeof(h) + (long)h.page_size_;
            break; /* a truncated file */
        }
        f = fopen("t_header.db", "r+b");
        CHECK(f && fwrite(&h, sizeof(h), 1U, f) == 1U);
        fclose(f);
        if (truncate_to >= 0)
            CHECK(truncate("t_header.db", truncate_to) == 0);
        CHECK(dbopen("t_header.db") == NULL);
    }
    printf("bad headers: ok\n");
}
//-------------------------------------------------------------------------------------------------------------
/* an insert that failed (the file was full) must not appear after a crash and recovery:
 * the keys of before the checkpoint are deleted after it, so the replay has room for it */
static void test_failed_insert_not_replayed(void **state)
{
    (void)state;
    const char *name = "t_failed.db";
    sDBC conf = tiny_cache_conf(); /* 128 cached pages, the file is bigger: evictions write the pages */
    conf.db_size = 400U * 512U;
    uchar_t v[40];
    memset(v, 'v', sizeof(v));
    sDB *db = dbcreate(name, &conf);
    CHECK(db);
    CHECK(db_close(db) == 0);

    int fds[2];
    CHECK(pipe(fds) == 0);
    fflush(NULL);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0)
    {
        in_child = true;
        if (!(db = dbopen(name)))
            _exit(2);
        uint32_t n = 0U;
        for (; n < 1000U; ++n)
            if (db_insert(db, &n, sizeof(n), v, sizeof(v)))
                _exit(3);
        if (db_flush(db)) /* a checkpoint: keys 0..999 are on disk, the journal starts after them */
            _exit(4);
        while (db_insert(db, &n, sizeof(n), v, sizeof(v)) == 0)
            if (++n > 100000U)
                _exit(5);
        /* n did not fit; free the room of the keys of before the checkpoint, the cache writes the pages */
        for (uint32_t k = 0U; k < 1000U; ++k)
            if (db_delete(db, &k, sizeof(k)))
                _exit(6);
        for (uint32_t k = 1000U; k < n; ++k)
        {
            void *val;
            size_t len;
            if (db_select(db, &k, sizeof(k), &val, &len) == 0)
                free(val);
        }
        if (write(fds[1], &n, sizeof(n)) != (ssize_t)sizeof(n))
            _exit(7);
        _exit(0); /* crash */
    }
    close(fds[1]);
    uint32_t failed = 0U;
    CHECK(read(fds[0], &failed, sizeof(failed)) == (ssize_t)sizeof(failed));
    close(fds[0]);
    int status = 0;
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    CHECK((db = dbopen(name)));
    void *val = NULL;
    size_t len = 0U;
    CHECK(db_select(db, &failed, sizeof(failed), &val, &len) != 0); /* the failed insert stays failed */
    for (uint32_t k = 0U; k < failed; ++k)
    {
        int found = db_select(db, &k, sizeof(k), &val, &len) == 0;
        if (found)
            free(val);
        CHECK(found == (k >= 1000U));
    }
    CHECK(db_check(db) == (long)(failed - 1000U));
    CHECK(db_close(db) == 0);
    printf("failed insert after a crash: ok (%u keys fit)\n", failed);
}
//-------------------------------------------------------------------------------------------------------------
/* a record cut by a full disk is removed from the journal: the records after it are recovered */
static void test_journal_short_write(void **state)
{
    (void)state;
    const char *name = "t_short.db";
    sDBC conf = small_conf();
    sDB *db = dbcreate(name, &conf);
    CHECK(db);
    CHECK(db_close(db) == 0);
    uchar_t v[40];
    memset(v, 'v', sizeof(v));

    fflush(NULL);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0)
    {
        in_child = true;
        if (!(db = dbopen(name)))
            _exit(2);
        for (uint32_t k = 0U; k < 100U; ++k)
            if (db_insert(db, &k, sizeof(k), v, sizeof(v)))
                _exit(3);
        struct stat st;
        if (stat("t_short.db.log", &st))
            _exit(4);
        /* the disk is full in the middle of the next record */
        signal(SIGXFSZ, SIG_IGN);
        struct rlimit lim, small;
        getrlimit(RLIMIT_FSIZE, &lim);
        small = lim;
        small.rlim_cur = (rlim_t)st.st_size + 30U;
        setrlimit(RLIMIT_FSIZE, &small);
        uint32_t k = 100U;
        if (db_insert(db, &k, sizeof(k), v, sizeof(v)) == 0)
            _exit(5); /* the journal record is not written: the insert must fail */
        setrlimit(RLIMIT_FSIZE, &lim);
        k = 101U;
        if (db_insert(db, &k, sizeof(k), v, sizeof(v)))
            _exit(6);
        _exit(0); /* crash */
    }
    int status = 0;
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    CHECK((db = dbopen(name)));
    for (uint32_t k = 0U; k <= 101U; ++k)
    {
        void *val = NULL;
        size_t len = 0U;
        int found = db_select(db, &k, sizeof(k), &val, &len) == 0;
        free(val);
        CHECK(found == (k != 100U));
    }
    CHECK(db_close(db) == 0);
    printf("short journal write: ok\n");
}
//-------------------------------------------------------------------------------------------------------------
static void test_random_default(void **state)
{
    (void)state;
    test_random("t_rand_default.db", small_conf(), 30000U, 0U);
}
static void test_random_tiny_cache(void **state)
{
    (void)state;
    test_random("t_rand_tiny.db", tiny_cache_conf(), 30000U, 0U);
}
static void test_random_reopen(void **state)
{
    (void)state;
    test_random("t_rand_reopen.db", tiny_cache_conf(), 20000U, 2500U);
}
static void test_random_4k_pages(void **state)
{
    (void)state;
    sDBC big = {0}; /* default 4 KB pages */
    big.db_size = 32U * 1024U * 1024U;
    test_random("t_rand_4k.db", big, 20000U, 7000U);
}
static void test_crash_recovery(void **state)
{
    (void)state;
    /* MYDB_CRASH_ROUNDS=N for a longer run */
    int rounds = getenv("MYDB_CRASH_ROUNDS") ? atoi(getenv("MYDB_CRASH_ROUNDS")) : 4;
    for (int round = 0; round < rounds; ++round)
        test_crash(round, round % 4 == 3);
}
//-------------------------------------------------------------------------------------------------------------
int main(void)
{
    rng_state = 0x0123456789ABCDEFULL;
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_basic),
        cmocka_unit_test(test_random_default),
        cmocka_unit_test(test_random_tiny_cache),
        cmocka_unit_test(test_random_reopen),
        cmocka_unit_test(test_random_4k_pages),
        cmocka_unit_test(test_delete_all),
        cmocka_unit_test(test_full),
        cmocka_unit_test(test_full_delete_some),
        cmocka_unit_test(test_open_bad_header),
        cmocka_unit_test(test_failed_insert_not_replayed),
        cmocka_unit_test(test_journal_short_write),
        cmocka_unit_test(test_crash_recovery),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
//-------------------------------------------------------------------------------------------------------------
