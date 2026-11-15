/**
 * @file mydb.h
 * @brief Public API of mydb: a B-tree key-value storage in a single file.
 *
 * Keys and values are arbitrary byte strings of variable length. Keys are
 * ordered by memcmp(), then by length. The storage is single-threaded.
 *
 * Every function returning @c int returns 0 on success and -1 on error;
 * mydb_errno (mydb_error.h) keeps the reason of the last error.
 */
#ifndef _MYDB_H_
#define _MYDB_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

// #define _DEBUG
#define MYDB_BITSINBYTE 8U
//-------------------------------------------------------------------------------------------------------------
/** @name Defaults, used when a field of sDBC is zero */
/** @{ */
#define MYDB_DEFAULT_DB_SIZE    (512U * 1024U * 1024U) /**< 512 MB */
#define MYDB_DEFAULT_PAGE_SIZE  (4U * 1024U)           /**< 4 KB */
#define MYDB_DEFAULT_CACHE_SIZE (16U * 1024U * 1024U)  /**< 16 MB */
/** @} */
/** @name Limits */
/** @{ */
#define MYDB_MIN_PAGE_SIZE   512U /**< the smallest page size */
#define MYDB_MIN_CACHE_PAGES 128U /**< a smaller cache is enlarged to this number of pages */
/** @} */
/** @name On-disk format: "MYDB" and the version */
/** @{ */
#define MYDB_MAGIC   0x4244594DU
#define MYDB_VERSION 2U
/** @} */
//-------------------------------------------------------------------------------------------------------------
/** Result of internal operations */
typedef enum db_state // : uchar_t
{
    DONE = 0,
    FAIL = -1
} eDBState;
//-------------------------------------------------------------------------------------------------------------
/** A key or a value: a byte string (see `man dbopen`) */
typedef struct DBT sDBT;
struct DBT
{
    void *data;  /**< bytes */
    size_t size; /**< number of bytes */
};
//-------------------------------------------------------------------------------------------------------------
/** Configuration of a new database; a zero field takes the default */
typedef struct DBC sDBC;
struct DBC
{
    size_t db_size;    /**< on-disk file size, below 4 GB; 512 MB by default */
    size_t page_size;  /**< page (B-tree node) size, at least MYDB_MIN_PAGE_SIZE; 4 KB by default */
    size_t cache_size; /**< memory for the page cache; 16 MB by default */
};
//-------------------------------------------------------------------------------------------------------------
/** Shape of the tree, see db_stats() */
typedef struct DBStat sDBStat;
struct DBStat
{
    unsigned height;           /**< levels: 1 for a single leaf */
    unsigned long keys;        /**< keys in the tree */
    unsigned long leaf_pages;  /**< leaf pages in the tree */
    unsigned long inner_pages; /**< inner pages in the tree */
    unsigned long empty_pages; /**< pages without keys (lazy deletion leaves them) */
    unsigned long used_bytes;  /**< bytes taken by the entries of all tree pages */
    unsigned long
        capacity_bytes;       /**< payload bytes of all tree pages: used_bytes / capacity_bytes is the fill */
    unsigned long file_pages; /**< data pages marked used in the free-page bitmap */
};
//-------------------------------------------------------------------------------------------------------------
/** The file header, at offset 0 */
typedef struct DBFileHeader sDBFH;
struct DBFileHeader
{
    uint32_t magic_;       /**< MYDB_MAGIC */
    uint32_t version_;     /**< MYDB_VERSION */
    uint32_t db_size_;     /**< const */
    uint32_t page_size_;   /**< const */
    uint32_t cache_size_;  /**< const */
    uint32_t block_count_; /**< const: number of data pages */
    uint32_t techb_count_; /**< const: number of bitmap pages */
    uint32_t nodes_count_; /**< used blocks, changed by the bitmap */
    uint32_t offset2root_; /**< the root block, changed when the tree grows or shrinks */
};
//-------------------------------------------------------------------------------------------------------------
typedef struct Block sBlock;
typedef struct Block sTechB;
typedef struct PageCache sCache;
typedef struct Logging sLog;

/** A database handle */
typedef struct DB sDB;
struct DB
{
    //---------------------------------------------
    /** @name Public API (the course interface): 0 on success, -1 on error */
    /** @{ */
    /** Flush and free the handle */
    int (*close)(struct DB *db);
    /** Delete the key; -1 if it is absent */
    int (*delete)(struct DB *db, const struct DBT *key);
    /** Insert the key or replace its value */
    int (*insert)(struct DB *db, const struct DBT *key, const struct DBT *data);
    /** Find the key; data->data is malloc'ed, the caller frees it */
    int (*select)(struct DB *db, const struct DBT *key, struct DBT *data);
    /** Checkpoint: write the cached pages to disk */
    int (*sync)(struct DB *db);
    /** @} */
    //---------------------------------------------
    /* Private */
    int hfile_; /* -1 when not opened */
    sDBFH head_;
    sBlock *root_;

    sCache *cache_;
    sLog *log_;
    bool replaying_; /* recovery in progress: do not journal */
    //---------------------------------------------
    sTechB *techb_array_;
    uint32_t techb_last0_;
};
//-------------------------------------------------------------------------------------------------------------
/**
 * Create a new database; an existing file is truncated.
 * The journal is "<file>.log".
 * @param file path of the database file
 * @param conf configuration, NULL for the defaults
 * @return the handle or NULL on error
 */
sDB *dbcreate(const char *file, const sDBC *conf);
/**
 * Open an existing database and recover it from the journal after a crash.
 * @return the handle or NULL (no file, not a mydb file, I/O error)
 */
sDB *dbopen(const char *file);

/** Checkpoint and free the handle; NULL is allowed */
int db_close(sDB *db);
/** Delete the key; -1 if there is no such key */
int db_delete(sDB *db, void *key, size_t key_len);
/**
 * Find the key.
 * @param[out] val     malloc'ed copy of the value (the caller frees it), NULL if not found
 * @param[out] val_len size of the value
 * @return 0 if found, -1 otherwise
 */
int db_select(sDB *db, void *key, size_t key_len, void **val, size_t *val_len);
/**
 * Insert the key or replace its value.
 * The key must not be empty; key_len + val_len must not exceed db_max_kv_size().
 */
int db_insert(sDB *db, void *key, size_t key_len, void *val, size_t val_len);
/** Checkpoint: write all dirty pages, fsync, truncate the journal */
int db_flush(sDB *db);
/** Check the B-tree invariants (sorted keys, bounds, equal leaf depth): number of keys or -1 */
long db_check(sDB *db);
/** Walk the tree and fill the statistics of its shape (height, pages, fill): 0 or -1 */
int db_stats(sDB *db, sDBStat *st);
/** The largest key + value size for the page size of this database */
size_t db_max_kv_size(const sDB *db);
/** Library version from the git tag it was built from: "0.0.5", "0.0.5-3-gabc1234" after the tag, "unknown" */
const char *db_version(void);
/** @name Trace levels for db_trace() and the MYDB_TRACE environment variable */
/** @{ */
#define MYDB_TRACE_OFF   0 /**< no trace */
#define MYDB_TRACE_OPS   1 /**< API operations, open, recovery, checkpoints */
#define MYDB_TRACE_TREE  2 /**< + the path through the tree, splits, key replacements, root changes */
#define MYDB_TRACE_PAGES 3 /**< + page cache misses and evictions, page allocation, journal records */
#define MYDB_TRACE_DUMP  4 /**< + the contents of every changed page */
/** @} */
/**
 * Trace what the tree is doing, one line per action, to debug without a debugger.
 * Without a call, dbcreate() / dbopen() read the environment: MYDB_TRACE=level,
 * MYDB_TRACE_FILE=path (appended; stderr by default).
 * @param level MYDB_TRACE_OFF .. MYDB_TRACE_DUMP
 * @param out   the stream, NULL for stderr
 */
void db_trace(int level, FILE *out);
//-------------------------------------------------------------------------------------------------------------
/** Compare keys: memcmp() of the common prefix, then the length; returns -1, 0 or 1 */
int key_compare(const sDBT *k, const sDBT *key);
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_H_
