/*  Unit tests of the internal modules: key comparison, entries inside a page,
 *  node split, free-page bitmap, page cache (hash table + LRU), journal records,
 *  configuration, the tree checker and the inner-node cases of deletion.
 *  Run from the tests/ directory: it creates u_*.db files.
 */
#define _XOPEN_SOURCE 700

#include "stdafx.h"
#include "mydb.h"
#include "mydb_low.h"
#include "mydb_error.h"
#include "mydb_block.h"
#include "mydb_block_low.h"
#include "mydb_techb.h"
#include "mydb_cache.h"
#include "mydb_log.h"
#include "mydb_log_low.h"
#include "mydb_trace.h"

//-------------------------------------------------------------------------------------------------------------
static int n_checks, n_failed;
#define CHECK(cond)                                                                                          \
    do                                                                                                       \
    {                                                                                                        \
        ++n_checks;                                                                                          \
        if (!(cond))                                                                                         \
        {                                                                                                    \
            ++n_failed;                                                                                      \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                         \
        }                                                                                                    \
    } while (0)

#define RUN(test)                                                                                            \
    do                                                                                                       \
    {                                                                                                        \
        int before = n_failed;                                                                               \
        test();                                                                                              \
        printf("%-28s %s\n", #test, (n_failed == before) ? "ok" : "FAILED");                                 \
    } while (0)

static sDBT S(const char *s)
{
    sDBT k = {(void *)s, strlen(s)};
    return k;
}
//-------------------------------------------------------------------------------------------------------------
/* A block over a local buffer, without a database */
#define PAGE 512U
typedef struct
{
    uint32_t mem[PAGE / sizeof(uint32_t)]; /* aligned for sDBHB */
    sBlock b;
} sFakeBlock;

static sBlock *fake_block(sFakeBlock *fb, eDBNT type, uint32_t offset)
{
    memset(fb, 0, sizeof(*fb));
    sBlock *b = &fb->b;
    b->memory_ = (uchar_t *)fb->mem;
    b->size_ = PAGE;
    b->offset_ = offset;
    b->is_mem_ = true;
    b->head_ = (sDBHB *)b->memory_;
    b->head_->node_type_ = type;
    b->head_->db_offset_ = offset;
    b->head_->free_size_ = PAGE - (uint32_t)sizeof(sDBHB);
    b->head_->pointer_0_ = MYDB_INVALIDPTR;
    b->data_ = b->memory_ + sizeof(sDBHB);
    b->free_ = b->data_;
    return b;
}
/* keys of the block as "a,b,c" */
static const char *keys_of(sBlock *b)
{
    static char out[4096];
    out[0] = '\0';
    sDBT iter = {0};
    for (sDBT *k = block_key_next(b, &iter, NULL); k; k = block_key_next(b, k, NULL))
    {
        if (out[0])
            strcat(out, ",");
        strncat(out, (const char *)k->data, k->size);
    }
    return out;
}
static sDBT find_key(sBlock *b, const char *key, sDBT *iter)
{
    sDBT want = S(key);
    memset(iter, 0, sizeof(*iter));
    for (sDBT *k = block_key_next(b, iter, NULL); k; k = block_key_next(b, k, NULL))
        if (!key_compare(k, &want))
            return *k;
    sDBT none = {NULL, 0};
    return none;
}
static bool value_is(sBlock *b, const char *key, const char *value)
{
    sDBT iter, k = find_key(b, key, &iter);
    if (!k.data)
        return false;
    sDBT v = block_key_data(b, &k);
    return v.size == strlen(value) && !memcmp(v.data, value, v.size);
}
static uint32_t used_bytes(sBlock *b)
{
    return (uint32_t)(b->free_ - b->data_);
}
//-------------------------------------------------------------------------------------------------------------
static void test_key_compare(void)
{
    sDBT a = S("abc"), ab = S("ab"), abd = S("abd"), b = S("b");
    CHECK(key_compare(&a, &a) == 0);
    CHECK(key_compare(&ab, &a) < 0 && key_compare(&a, &ab) > 0); /* prefix is smaller */
    CHECK(key_compare(&a, &abd) < 0 && key_compare(&abd, &a) > 0);
    CHECK(key_compare(&abd, &b) < 0); /* bytes first, then length */

    uchar_t lo[] = {0x00, 0x01}, hi[] = {0xFF};
    sDBT klo = {lo, 2}, khi = {hi, 1};
    CHECK(key_compare(&klo, &khi) < 0); /* unsigned bytes */
    CHECK(key_compare(&khi, &klo) == 1 && key_compare(&klo, &khi) == -1);
}
//-------------------------------------------------------------------------------------------------------------
static void test_page_insert_iterate(void)
{
    sFakeBlock fb;
    sBlock *b = fake_block(&fb, Leaf, 0);
    sDBT v = S("v");

    CHECK(block_key_next(b, &(sDBT){0}, NULL) == NULL); /* empty */
    const char *keys[] = {"m", "c", "x", "a", "mm", "b"};
    for (size_t i = 0; i < sizeof(keys) / sizeof(*keys); ++i)
    {
        sDBT k = S(keys[i]);
        CHECK(block_insert(b, &k, &v, NULL, MYDB_INVALIDPTR) == DONE);
    }
    CHECK(*block_nkvs(b) == 6U);
    CHECK(!strcmp(keys_of(b), "a,b,c,m,mm,x"));
    /* free size and the free pointer agree */
    CHECK(b->head_->free_size_ + used_bytes(b) == PAGE - sizeof(sDBHB));
    CHECK(used_bytes(b) == 6U * MYDB_ENTRY_OVERHEAD + 7U + 6U);

    /* sizes read from the entries */
    uint_t vsz = 0;
    sDBT iter = {0}, *k = block_key_next(b, &iter, &vsz);
    CHECK(k && k->size == 1U && vsz == 1U);
}
static void test_page_update(void)
{
    sFakeBlock fb;
    sBlock *b = fake_block(&fb, Leaf, 0);
    sDBT k1 = S("k1"), k2 = S("k2"), k3 = S("k3"), small = S("x"), big = S("a much longer value");
    CHECK(block_insert(b, &k1, &small, NULL, MYDB_INVALIDPTR) == DONE);
    CHECK(block_insert(b, &k2, &small, NULL, MYDB_INVALIDPTR) == DONE);
    CHECK(block_insert(b, &k3, &small, NULL, MYDB_INVALIDPTR) == DONE);
    uint32_t used = used_bytes(b);

    /* grow and shrink the middle value: neighbours stay intact */
    CHECK(block_insert(b, &k2, &big, NULL, MYDB_INVALIDPTR) == DONE);
    CHECK(*block_nkvs(b) == 3U);
    CHECK(value_is(b, "k2", "a much longer value") && value_is(b, "k1", "x") && value_is(b, "k3", "x"));
    CHECK(used_bytes(b) == used + big.size - small.size);
    CHECK(block_insert(b, &k2, &small, NULL, MYDB_INVALIDPTR) == DONE);
    CHECK(used_bytes(b) == used && value_is(b, "k2", "x") && value_is(b, "k3", "x"));
}
static void test_page_full(void)
{
    sFakeBlock fb;
    sBlock *b = fake_block(&fb, Leaf, 0);
    char key[16];
    uchar_t val[40] = {0};
    sDBT v = {val, sizeof(val)};
    int n = 0;
    for (;; ++n)
    {
        snprintf(key, sizeof(key), "key%03d", n);
        sDBT k = S(key);
        if (block_insert(b, &k, &v, NULL, MYDB_INVALIDPTR) != DONE)
            break;
    }
    CHECK(n == (int)((PAGE - sizeof(sDBHB)) / (MYDB_ENTRY_OVERHEAD + 6U + sizeof(val))));
    CHECK(*block_nkvs(b) == (uint32_t)n); /* the failed insert changed nothing */
    CHECK(block_isfull(b));
}
static void test_page_pointers(void)
{
    sFakeBlock fb;
    sBlock *b = fake_block(&fb, Pass, 0);
    sDBT a = S("a"), c = S("c"), e = S("e"), v = S("");
    b->head_->pointer_0_ = 10;
    CHECK(block_insert(b, &c, &v, NULL, 30) == DONE);
    CHECK(block_insert(b, &a, &v, NULL, 20) == DONE);
    CHECK(block_insert(b, &e, &v, NULL, 40) == DONE);
    /* | p0=10 | a | 20 | c | 30 | e | 40 | */
    sDBT it, ka = find_key(b, "a", &it), kc, ke;
    kc = find_key(b, "c", &it);
    ke = find_key(b, "e", &it);
    CHECK(block_lptr(b, &ka) == 10 && block_rptr(b, &ka) == 20);
    CHECK(block_lptr(b, &kc) == 20 && block_rptr(b, &kc) == 30);
    CHECK(block_lptr(b, &ke) == 30 && block_rptr(b, &ke) == 40);
    CHECK(block_lptr(b, NULL) == 40 && block_rptr(b, NULL) == 10); /* the very right / left child */

    /* change a key in place: the right pointer is kept */
    sDBT d = S("dd"), dv = S("value");
    kc = find_key(b, "c", &it);
    CHECK(block_change(b, &d, &dv, &kc, MYDB_INVALIDPTR) == DONE);
    CHECK(!strcmp(keys_of(b), "a,dd,e"));
    kc = find_key(b, "dd", &it);
    CHECK(block_rptr(b, &kc) == 30 && value_is(b, "dd", "value"));
    ke = find_key(b, "e", &it);
    CHECK(block_rptr(b, &ke) == 40);

    /* delete with the right pointer: "dd" and 30 go away */
    CHECK(block_delete(b, &d, false) == DONE);
    ke = find_key(b, "e", &it);
    CHECK(!strcmp(keys_of(b), "a,e") && *block_nkvs(b) == 2U && block_lptr(b, &ke) == 20);
    /* delete the first key with its left pointer: 10 goes away, 20 becomes pointer_0_ */
    CHECK(block_delete(b, &a, true) == DONE);
    CHECK(!strcmp(keys_of(b), "e") && b->head_->pointer_0_ == 20);
    CHECK(block_delete(b, &a, true) == FAIL); /* absent */
    CHECK(b->head_->free_size_ + used_bytes(b) == PAGE - sizeof(sDBHB));
}
static void test_page_change_no_room(void)
{
    sFakeBlock fb;
    sBlock *b = fake_block(&fb, Leaf, 0);
    uchar_t val[200] = {0};
    sDBT k1 = S("k1"), k2 = S("k2"), v = {val, sizeof(val)}, small = S("s");
    CHECK(block_insert(b, &k1, &v, NULL, MYDB_INVALIDPTR) == DONE);
    CHECK(block_insert(b, &k2, &small, NULL, MYDB_INVALIDPTR) == DONE);
    sDBT it, k = find_key(b, "k2", &it);
    sDBT huge = {val, sizeof(val) + b->head_->free_size_};
    CHECK(block_change(b, &k2, &huge, &k, MYDB_INVALIDPTR) == FAIL);
    CHECK(value_is(b, "k2", "s"));
}
//-------------------------------------------------------------------------------------------------------------
static void test_split(void)
{
    sFakeBlock fx, fy, fz;
    sBlock *x = fake_block(&fx, Pass, 1), *y = fake_block(&fy, Leaf, 2), *z = fake_block(&fz, Leaf, 3);
    x->head_->pointer_0_ = 2;

    char key[16];
    uchar_t val[30] = {0};
    sDBT v = {val, sizeof(val)};
    int n = 0;
    while (!block_isfull(y))
    {
        snprintf(key, sizeof(key), "k%03d", n++);
        sDBT k = S(key);
        CHECK(block_insert(y, &k, &v, NULL, MYDB_INVALIDPTR) == DONE);
    }
    CHECK(block_split_child(x, y, z, NULL) == DONE);
    CHECK(*block_nkvs(x) == 1U && *block_type(z) == Leaf);
    CHECK(*block_nkvs(y) + *block_nkvs(z) + 1U == (uint32_t)n);
    /* halves of about the same size */
    CHECK(used_bytes(y) >= used_bytes(z) &&
          used_bytes(y) - used_bytes(z) <= 2U * (MYDB_ENTRY_OVERHEAD + 4U + 30U));
    /* y < median < z, the median points to z */
    sDBT it = {0}, *m = block_key_next(x, &it, NULL);
    CHECK(m && block_lptr(x, m) == 2 && block_rptr(x, m) == 3);
    sDBT ylast = {0}, iy = {0}, iz = {0};
    for (sDBT *k = block_key_next(y, &iy, NULL); k; k = block_key_next(y, k, NULL))
        ylast = *k;
    sDBT *zfirst = block_key_next(z, &iz, NULL);
    CHECK(key_compare(&ylast, m) < 0 && key_compare(m, zfirst) < 0);
    CHECK(y->dirty_ && z->dirty_ && x->dirty_);
    CHECK(!block_isfull(y) && !block_isfull(z));
}
//-------------------------------------------------------------------------------------------------------------
/* sequential inserts: a key beyond the edge of the full block splits it at that edge */
static void test_split_at_edge(void)
{
    for (int ascending = 0; ascending < 2; ++ascending)
    {
        sFakeBlock fx, fy, fz;
        sBlock *x = fake_block(&fx, Pass, 1), *y = fake_block(&fy, Leaf, 2), *z = fake_block(&fz, Leaf, 3);
        x->head_->pointer_0_ = 2;

        char key[16];
        uchar_t val[30] = {0};
        sDBT v = {val, sizeof(val)};
        int n = 0;
        while (!block_isfull(y))
        {
            snprintf(key, sizeof(key), "k%03d", 100 + n++);
            sDBT k = S(key);
            CHECK(block_insert(y, &k, &v, NULL, MYDB_INVALIDPTR) == DONE);
        }
        uint32_t used = used_bytes(y);
        sDBT next = S(ascending ? "k999" : "k000");
        CHECK(block_split_child(x, y, z, &next) == DONE);
        CHECK(*block_nkvs(x) == 1U);
        sDBT it = {0}, *m = block_key_next(x, &it, NULL);
        if (ascending)
        { /* everything but the last entry stays, the new keys go to the empty z */
            CHECK(*block_nkvs(y) == (uint32_t)n - 1U && *block_nkvs(z) == 0U);
            CHECK(m && !memcmp(m->data, key, m->size));
            CHECK(used_bytes(y) + MYDB_ENTRY_OVERHEAD + 4U + sizeof(val) == used);
        }
        else
        { /* the first entry goes up, the rest to z, y is empty */
            CHECK(*block_nkvs(y) == 0U && *block_nkvs(z) == (uint32_t)n - 1U);
            CHECK(m && !memcmp(m->data, "k100", m->size));
        }
        CHECK(key_compare(&next, m) == (ascending ? 1 : -1));
        CHECK(block_lptr(x, m) == 2 && block_rptr(x, m) == 3);
    }
}
static sDB *small_db(const char *name, size_t db_size)
{
    sDBC conf = {db_size, PAGE, 1}; /* the smallest cache */
    return dbcreate(name, &conf);
}
static void test_bitmap(void)
{
    sDB *db = small_db("u_bitmap.db", 256U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    uint32_t count = db->head_.block_count_;
    CHECK(db->head_.techb_count_ * PAGE * 8U >= count);
    CHECK(db->head_.nodes_count_ == 1U); /* the root */

    bool bit = false;
    CHECK(techb_get_bit(db, db->head_.offset2root_, &bit) == DONE && bit);
    uint32_t a = techb_get_index_of_first_free_bit(db);
    uint32_t b = techb_get_index_of_first_free_bit(db);
    CHECK(a == 1U && b == 2U && db->head_.nodes_count_ == 3U);
    CHECK(techb_get_bit(db, a, &bit) == DONE && bit);

    CHECK(techb_set_bit(db, a, false) == DONE);
    CHECK(techb_get_bit(db, a, &bit) == DONE && !bit && db->head_.nodes_count_ == 2U);
    CHECK(techb_get_index_of_first_free_bit(db) == a); /* the lowest free block is reused */

    CHECK(techb_get_bit(db, count, &bit) == FAIL); /* out of range */
    CHECK(techb_set_bit(db, count, true) == FAIL);

    uint32_t got = 0;
    while (techb_get_index_of_first_free_bit(db) < count)
        ++got;
    CHECK(got == count - 3U && db->head_.nodes_count_ == count);
    CHECK(techb_get_index_of_first_free_bit(db) == count); /* full */

    /* the bitmap reaches the disk */
    CHECK(techb_set_bit(db, 5, false) == DONE && db_close(db) == 0);
    CHECK((db = dbopen("u_bitmap.db")) != NULL);
    if (db)
    {
        CHECK(techb_get_index_of_first_free_bit(db) == 5U);
        db_close(db);
    }
}
//-------------------------------------------------------------------------------------------------------------
/* The cache against a model LRU list */
static void test_cache_lru(void)
{
    sDB *db = small_db("u_cache.db", 2U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    sBlock *saved_root = db->root_;
    db->root_ = NULL;           /* the root has a special treatment in the cache */
    CHECK(mydb_flush(db) == 0); /* all clean: evictions write nothing */

    uint32_t cap = db->cache_->all_pgs;
    CHECK(cap == MYDB_MIN_CACHE_PAGES);
    uint32_t *lru = calloc(cap, sizeof(*lru)); /* lru[0] - the most recent */
    uint32_t n = 0, base = 1000U, mismatches = 0;
    uint64_t rnd = 12345;

    for (int step = 0; step < 20000; ++step)
    {
        uint32_t off;
        if (step < (int)cap)
            off = base + (uint32_t)step; /* fill the cache with fresh pages */
        else
        {
            rnd = rnd * 6364136223846793005ULL + 1442695040888963407ULL;
            off = base + (uint32_t)((rnd >> 33) % (3U * cap));
        }
        uint32_t pos = n;
        for (uint32_t i = 0; i < n; ++i)
            if (lru[i] == off)
                pos = i;
        bool expect_hit = (pos < n);

        sBlock *blk = NULL;
        bool hit = mydb_cache_push(db, off, &blk);
        if (hit != expect_hit || !blk || blk->offset_ != off)
        {
            if (!mismatches)
                fprintf(stderr, "cache: step %d page %u hit %d, expected %d\n", step, off, hit, expect_hit);
            ++mismatches;
        }
        if (!hit && blk)
        { /* a clean "block" with a marker in its memory */
            blk->dirty_ = false;
            mydb_wr32(blk->memory_, off);
        }
        else if (blk && mydb_rd32(blk->memory_) != off)
            ++mismatches; /* the hit returned the memory of another page */

        if (!expect_hit)
            pos = (n < cap) ? n++ : cap - 1U; /* the tail is evicted */
        memmove(&lru[1], &lru[0], pos * sizeof(*lru));
        lru[0] = off;
    }
    CHECK(mismatches == 0U);
    free(lru);
    /* the cached memory is garbage now: do not let anything use it */
    mydb_cache_free(db);
    CHECK(!mydb_cache_init(db));
    (void)saved_root;
    CHECK((db->root_ = block_create(db, db->head_.offset2root_)) != NULL);
    CHECK(db_check(db) == 0);
    db_close(db);
}
/* dirty pages pushed out of the cache are read back intact */
static void test_cache_writeback(void)
{
    sDB *db = small_db("u_wb.db", 2U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    enum
    {
        N = 3 * MYDB_MIN_CACHE_PAGES
    };
    uint32_t offs[N];
    for (uint32_t i = 0; i < N; ++i)
    {
        sBlock *b = block_create(db, MYDB_OFFSET2NEW);
        CHECK(b != NULL);
        if (!b)
            return;
        offs[i] = b->offset_;
        uint32_t data = i * 7U;
        sDBT k = {&i, sizeof(i)}, v = {&data, sizeof(data)};
        CHECK(block_insert(b, &k, &v, NULL, MYDB_INVALIDPTR) == DONE);
        block_write(b);
    }
    for (uint32_t i = 0; i < N; ++i)
    {
        sBlock *b = block_create(db, offs[i]);
        CHECK(b && *block_nkvs(b) == 1U);
        if (!b)
            continue;
        sDBT it = {0}, *k = block_key_next(b, &it, NULL);
        sDBT v = block_key_data(b, k);
        CHECK(k->size == 4U && mydb_rd32(k->data) == i && mydb_rd32(v.data) == i * 7U);
    }
    /* reading a page that is not a node is an error, not a crash */
    uint32_t free_off = techb_get_index_of_first_free_bit(db);
    techb_set_bit(db, free_off, false);
    CHECK(block_create(db, free_off) == NULL);
    CHECK(block_create(db, db->head_.block_count_) == NULL);
    db_close(db);
}
//-------------------------------------------------------------------------------------------------------------
static void test_log_records(void)
{
    sLog *log = mydb_log_open("u_log.db", true);
    CHECK(log != NULL);
    if (!log)
        return;
    sLogRec r1 = {LOG_INSERT, 0, S("key"), S("value"), NULL};
    sLogRec r2 = {LOG_DELETE, 0, S("other"), {NULL, 0}, NULL};
    sLogRec r3 = {LOG_CHKPNT, 0, {NULL, 0}, {NULL, 0}, NULL};
    CHECK(!mydb_log_write(log, &r1) && !mydb_log_write(log, &r2) && !mydb_log_write(log, &r3));
    CHECK(r1.lsn == 1U && r2.lsn == 2U && r3.lsn == 3U);
    size_t len1 = MYDB_LOG_HEAD_SIZE + 3U + 5U + 4U, len2 = MYDB_LOG_HEAD_SIZE + 5U + 4U;
    CHECK(lseek(log->fd, 0, SEEK_END) == (off_t)(len1 + len2 + MYDB_LOG_HEAD_SIZE + 4U));

    lseek(log->fd, 0, SEEK_SET);
    sLogRec *a = mydb_log_record_next(log), *b = mydb_log_record_next(log), *c = mydb_log_record_next(log);
    CHECK(a && a->type == LOG_INSERT && a->lsn == 1U && a->key.size == 3U && !memcmp(a->key.data, "key", 3) &&
          a->val.size == 5U && !memcmp(a->val.data, "value", 5));
    CHECK(b && b->type == LOG_DELETE && b->lsn == 2U && b->key.size == 5U && b->val.size == 0U);
    CHECK(c && c->type == LOG_CHKPNT && c->lsn == 3U);
    CHECK(mydb_log_record_next(log) == NULL); /* the end */
    mydb_log_record_free(a);
    mydb_log_record_free(b);
    mydb_log_record_free(c);

    /* a corrupted byte in the 2nd record: the checksum stops the reading there */
    /* (pwrite on the O_APPEND descriptor of the journal would append on Linux) */
    uchar_t byte = 0;
    int fd = open("u_log.db.log", O_RDWR);
    CHECK(fd != -1 && pread(fd, &byte, 1, (off_t)(len1 + MYDB_LOG_HEAD_SIZE)) == 1);
    byte ^= 0x20;
    CHECK(pwrite(fd, &byte, 1, (off_t)(len1 + MYDB_LOG_HEAD_SIZE)) == 1);
    close(fd);
    lseek(log->fd, 0, SEEK_SET);
    a = mydb_log_record_next(log);
    CHECK(a != NULL && mydb_log_record_next(log) == NULL);
    mydb_log_record_free(a);

    /* a torn record */
    CHECK(ftruncate(log->fd, (off_t)(len1 - 2U)) == 0);
    lseek(log->fd, 0, SEEK_SET);
    CHECK(mydb_log_record_next(log) == NULL);

    close(log->fd);
    free(log->filename);
    free(log);
}
static void test_log_replay(void)
{
    /* operations after the last checkpoint are replayed on open */
    sDB *db = small_db("u_replay.db", 1U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    CHECK(db_insert(db, "before", 6U, "1", 1U) == 0 && db_flush(db) == 0);
    CHECK(db_insert(db, "after", 5U, "2", 1U) == 0 && db_delete(db, "before", 6U) == 0);
    /* forget the cached pages: as if the process died now */
    sDB copy = *db;
    db->log_ = NULL;
    db->root_ = NULL; /* db_close () must not flush */
    close(copy.log_->fd);
    free(copy.log_->filename);
    free(copy.log_);
    db_close(db);

    CHECK((db = dbopen("u_replay.db")) != NULL);
    if (!db)
        return;
    void *v = NULL;
    size_t len = 0;
    CHECK(db_select(db, "after", 5U, &v, &len) == 0 && len == 1U && *(char *)v == '2');
    free(v);
    CHECK(db_select(db, "before", 6U, &v, &len) != 0);
    CHECK(db_check(db) == 1);
    db_close(db);
}
//-------------------------------------------------------------------------------------------------------------
static void test_config(void)
{
    sDB *db = dbcreate("u_default.db", NULL); /* a sparse 512 MB file */
    CHECK(db != NULL);
    if (db)
    {
        CHECK(db->head_.page_size_ == MYDB_DEFAULT_PAGE_SIZE && db->head_.db_size_ == MYDB_DEFAULT_DB_SIZE);
        CHECK(db->head_.magic_ == MYDB_MAGIC && db->head_.version_ == MYDB_VERSION);
        uint32_t pages = db->head_.techb_count_ + db->head_.block_count_;
        CHECK(pages == (MYDB_DEFAULT_DB_SIZE - sizeof(sDBFH)) / MYDB_DEFAULT_PAGE_SIZE);
        CHECK(db->head_.techb_count_ == 4U); /* 131071 pages, 32768 bits per bitmap page */
        CHECK(db->cache_->all_pgs == MYDB_DEFAULT_CACHE_SIZE / MYDB_DEFAULT_PAGE_SIZE);
        CHECK(db_max_kv_size(db) == (MYDB_DEFAULT_PAGE_SIZE - sizeof(sDBHB)) / 8U - MYDB_ENTRY_OVERHEAD);
        struct stat st;
        CHECK(stat("u_default.db", &st) == 0 &&
              st.st_size == (off_t)(sizeof(sDBFH) + (size_t)pages * MYDB_DEFAULT_PAGE_SIZE));
        db_close(db);
    }
    unlink("u_default.db");
    unlink("u_default.db.log");

    sDBC bad_page = {1U << 20, 100U, 0}, too_small = {1000U, 512U, 0}, too_big = {(size_t)5 << 30, 0, 0};
    CHECK(dbcreate("u_bad.db", &bad_page) == NULL);
    CHECK(dbcreate("u_bad.db", &too_small) == NULL);
    if (sizeof(size_t) > 4U)
        CHECK(dbcreate("u_bad.db", &too_big) == NULL);
    CHECK(dbcreate(NULL, NULL) == NULL && dbopen(NULL) == NULL);
    CHECK(db_close(NULL) == 0);
}
static void test_errors(void)
{
    for (int e = MYDB_ERR_FPARAM; e <= MYDB_ERR_LOGWRT; ++e)
    {
        mydb_errno = (e_mydb_err)e;
        const char *s = strmyerror((e_mydb_err)e);
        CHECK(s != NULL && s[0] == ':' && mydb_errno == MYDB_ERR_NONE);
    }
    CHECK(strmyerror(MYDB_ERR_NONE) == NULL);

    sDB *db = small_db("u_err.db", 1U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    uchar_t big[1024] = {0};
    mydb_errno = MYDB_ERR_NONE;
    CHECK(db_insert(db, "k", 1U, big, db_max_kv_size(db)) != 0 && mydb_errno == MYDB_ERR_KVSIZE);
    CHECK(db_insert(db, "k", 0U, "v", 1U) != 0);
    void *val = big;
    size_t len = 1U;
    CHECK(db_select(db, NULL, 1U, &val, &len) != 0 && val == NULL && len == 0U);
    db_close(db);
}
//-------------------------------------------------------------------------------------------------------------
static void test_check_detects_corruption(void)
{
    sDB *db = small_db("u_corrupt.db", 1U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    const char *keys[] = {"a", "b", "c", "d"};
    for (int i = 0; i < 4; ++i)
        CHECK(db_insert(db, (void *)keys[i], 1U, "v", 1U) == 0);
    CHECK(db_check(db) == 4);

    sBlock *root = block_create(db, db->head_.offset2root_);
    sDBT it, k = find_key(root, "a", &it);
    *(char *)k.data = 'z'; /* "z,b,c,d": not sorted */
    CHECK(db_check(db) == -1);
    *(char *)k.data = 'a';
    ++root->head_->kvs_count_; /* the counter does not match */
    CHECK(db_check(db) == -1);
    --root->head_->kvs_count_;
    CHECK(db_check(db) == 4);
    db_close(db);
}
/* height of the tree: the number of blocks from the root to a leaf */
static int tree_height(sDB *db)
{
    int h = 1;
    sBlock *b = block_create(db, db->head_.offset2root_);
    while (b && *block_type(b) == Pass)
    {
        b = block_create(db, b->head_->pointer_0_);
        ++h;
    }
    return h;
}
static void test_inner_node_cases(void)
{
    sDB *db = small_db("u_inner.db", 4U * 1024U * 1024U);
    CHECK(db != NULL);
    if (!db)
        return;
    enum
    {
        N = 3000
    };
    char key[16];
    for (int i = 0; i < N; ++i)
    {
        snprintf(key, sizeof(key), "key%05d", i);
        CHECK(db_insert(db, key, strlen(key), key, strlen(key)) == 0);
    }
    CHECK(db_check(db) == N && tree_height(db) >= 3);

    /* update a key stored in the root: no duplicate in a leaf */
    sBlock *root = block_create(db, db->head_.offset2root_);
    sDBT it = {0}, *rk = block_key_next(root, &it, NULL);
    char inner[16] = {0};
    memcpy(inner, rk->data, rk->size);
    CHECK(db_insert(db, inner, strlen(inner), "updated", 7U) == 0);
    CHECK(db_check(db) == N);
    void *v = NULL;
    size_t len = 0;
    CHECK(db_select(db, inner, strlen(inner), &v, &len) == 0 && len == 7U && !memcmp(v, "updated", 7));
    free(v);

    /* delete the key from the root: replaced by its predecessor */
    CHECK(db_delete(db, inner, strlen(inner)) == 0);
    CHECK(db_check(db) == N - 1 && db_select(db, inner, strlen(inner), &v, &len) != 0);
    CHECK(db_delete(db, inner, strlen(inner)) != 0);

    /* delete everything: all blocks but the root leaf are freed */
    for (int i = 0; i < N; ++i)
    {
        snprintf(key, sizeof(key), "key%05d", i);
        db_delete(db, key, strlen(key));
    }
    CHECK(db_check(db) == 0 && tree_height(db) == 1 && db->head_.nodes_count_ == 1U);
    db_close(db);
}
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
static void test_trace(void)
{
    char buf[MYDB_TRACE_KEYBUF];
    CHECK(!strcmp(mydb_trace_key("kiwi", 4U, buf), "\"kiwi\""));
    CHECK(!strcmp(mydb_trace_key("\x01\xff", 2U, buf), "0x01ff"));
    char longkey[200];
    memset(longkey, 'z', sizeof(longkey));
    CHECK(strlen(mydb_trace_key(longkey, sizeof(longkey), buf)) < MYDB_TRACE_KEYBUF);
    CHECK(strstr(buf, "...") != NULL);
    //-----------------------------------------
    FILE *f = tmpfile();
    CHECK(f != NULL);
    if (!f)
        return;
    db_trace(MYDB_TRACE_DUMP, f);
    sDB *db = small_db("u_trace.db", 1U << 20);
    CHECK(db != NULL);
    char key[16];
    for (int i = 0; i < 300; ++i)
    {
        snprintf(key, sizeof(key), "key%05d", i);
        CHECK(db_insert(db, key, strlen(key), "value", 5U) == 0);
    }
    for (int i = 0; i < 300; ++i)
    {
        snprintf(key, sizeof(key), "key%05d", i);
        CHECK(db_delete(db, key, strlen(key)) == 0);
    }
    CHECK(db_check(db) == 0);
    db_close(db);
    //-----------------------------------------
    long size = ftell(f);
    db_trace(MYDB_TRACE_OFF, NULL);
    db = small_db("u_trace.db", 1U << 20);
    db_close(db);
    CHECK(ftell(f) == size); /* nothing is written with the trace off */
    //-----------------------------------------
    char *text = calloc((size_t)size + 1U, 1U);
    CHECK(text != NULL);
    if (text)
    {
        rewind(f);
        CHECK(fread(text, 1U, (size_t)size, f) == (size_t)size);
        const char *expected[] = {"create 'u_trace.db'", "insert key=\"key00000\" (8 B) value 5 B",
                                  "journal: LSN",        "allocate page",
                                  "cache miss page",     "split leaf page",
                                  "the tree grows",      "page 0 after insert",
                                  "replace \"key",       "release page",
                                  "the tree is lower",   "delete done",
                                  "checkpoint",          "close"};
        for (size_t i = 0U; i < sizeof(expected) / sizeof(*expected); ++i)
        {
            if (!strstr(text, expected[i]))
                fprintf(stderr, "no '%s' in the trace\n", expected[i]);
            CHECK(strstr(text, expected[i]) != NULL);
        }
        free(text);
    }
    fclose(f);
}
int main(void)
{
    RUN(test_key_compare);
    RUN(test_page_insert_iterate);
    RUN(test_page_update);
    RUN(test_page_full);
    RUN(test_page_pointers);
    RUN(test_page_change_no_room);
    RUN(test_split);
    RUN(test_split_at_edge);
    RUN(test_bitmap);
    RUN(test_cache_lru);
    RUN(test_cache_writeback);
    RUN(test_log_records);
    RUN(test_log_replay);
    RUN(test_config);
    RUN(test_errors);
    RUN(test_check_detects_corruption);
    RUN(test_inner_node_cases);
    RUN(test_trace);
    printf("%d checks, %d failed\n", n_checks, n_failed);
    return n_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
//-------------------------------------------------------------------------------------------------------------
