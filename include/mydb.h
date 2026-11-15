/**
 * @file mydb.h
 * @brief Public API of mydb: a B-tree key-value storage in a single file.
 *
 * Keys and values are arbitrary byte strings of variable length. Keys are
 * ordered by memcmp(), then by length. The storage is single-threaded.
 *
 * Every function returning @c int returns 0 on success and -1 on error;
 * db_error() returns the reason of the last error.
 */
#ifndef _MYDB_H_
#define _MYDB_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

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
//-------------------------------------------------------------------------------------------------------------
/** Reason of the last error, see db_error() */
typedef enum mydb_error
{
    MYDB_ERR_NONE,   /**< no error */
    MYDB_ERR_FPARAM, /**< an incorrect parameter (configuration) */
    MYDB_ERR_FNEXST, /**< the file does not exist or is not accessible */
    MYDB_ERR_BWRITE, /**< writing pages to disk failed */
    MYDB_ERR_OFFSET, /**< a page number out of the file */
    MYDB_ERR_NFREEB, /**< no free page: the file is full */
    MYDB_ERR_NFREES, /**< no free space in a page */
    MYDB_ERR_CCHSET, /**< the page cache failed */
    MYDB_ERR_KVSIZE, /**< an empty key or a key + value bigger than db_max_kv_size() */
    MYDB_ERR_FORMAT, /**< not a mydb file or a broken one */
    MYDB_ERR_LOGWRT  /**< writing the journal failed */
} e_mydb_err;
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
    unsigned long empty_pages; /**< pages without keys: none after the rebalancing deletion */
    unsigned long used_bytes;  /**< bytes taken by the entries of all tree pages */
    unsigned long
        capacity_bytes;       /**< payload bytes of all tree pages: used_bytes / capacity_bytes is the fill */
    unsigned long file_pages; /**< data pages marked used in the free-page bitmap */
};
//-------------------------------------------------------------------------------------------------------------
/** A database handle. The API sees the function pointers only; the fields of the implementation
 *  are added when the library includes its internal header (src/mydb_common.h) first. */
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
#ifdef MYDB_PRIVATE_FIELDS
    MYDB_PRIVATE_FIELDS
#endif
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
/** Reason of the last error (e_mydb_err); it is kept until the next error */
int db_error(void);
/** Text of an error code, "" for MYDB_ERR_NONE */
const char *db_strerror(int err);
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
