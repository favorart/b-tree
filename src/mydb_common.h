/*  Common definitions of the implementation (not a part of the public API). */
#ifndef _MYDB_COMMON_H_
#define _MYDB_COMMON_H_
//-------------------------------------------------------------------------------------------------------------
#if !defined(__unix__) && !defined(__APPLE__)
#error mydb needs a POSIX system
#endif
/* files up to 4 GB on 32-bit systems too */
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
//-------------------------------------------------------------------------------------------------------------
/* markers of the input and output parameters */
#define IN
#define OUT
//-------------------------------------------------------------------------------------------------------------
/* offsets in the file go up to 4 GB: a 32-bit off_t would wrap at 2 GB (C99 has no static_assert) */
typedef char mydb_off_t_is_64_bits[(sizeof(off_t) >= 8U) ? 1 : -1];
typedef unsigned char uchar_t;
typedef uint32_t uint_t;
//-------------------------------------------------------------------------------------------------------------
/* Unaligned-safe access to the 32-bit fields packed inside a page */
static inline uint32_t mydb_rd32(const void *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}
static inline void mydb_wr32(void *p, uint32_t v)
{
    memcpy(p, &v, sizeof(v));
}
//-------------------------------------------------------------------------------------------------------------
/* the implementation part of the public types (include/mydb.h) */
#define MYDB_BITSINBYTE    8U
#define MYDB_MAX_PAGE_SIZE (1U << 24) /* 16 MB */
/* a checkpoint when the journal grows over 2 * cache size, but at least this */
#define MYDB_LOG_LIMIT_MIN (4 * 1024 * 1024)
/* fsync (), replaced by the tests to simulate a failing disk */
extern int (*mydb_fsync)(int fd);
/*  On-disk format: "MYDB" and the version */
#define MYDB_MAGIC   0x4244594DU
#define MYDB_VERSION 2U
/** Result of internal operations */
typedef enum db_state
{
    DONE = 0,
    FAIL = -1
} eDBState;
/** The file header, at offset 0 */
//-------------------------------------------------------------------------------------------------------------
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
/* the private fields of struct DB */
#define MYDB_PRIVATE_FIELDS                                                                                  \
    int hfile_; /* -1 when not opened */                                                                     \
    sDBFH head_;                                                                                             \
    sBlock *root_;                                                                                           \
    sCache *cache_;                                                                                          \
    sLog *log_;                                                                                              \
    bool replaying_; /* recovery in progress: do not journal */                                              \
    sTechB *techb_array_;                                                                                    \
    uint32_t techb_last0_;                                                                                   \
    uint32_t height_; /* levels of the tree: the pages a split chain may need */
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_COMMON_H_
