# mydb — a B-tree key-value storage in C

[![CI](https://github.com/favorart/b-tree/actions/workflows/ci.yml/badge.svg)](https://github.com/favorart/b-tree/actions/workflows/ci.yml)

A small embedded key-value storage: a single file of fixed-size pages
organized as a B-tree, with variable-length keys and values,
an LRU write-back page cache and a write-ahead journal for crash recovery.
Plain C99 and POSIX, no dependencies.

It was written in 2015 as a course project at Technosphere,
the educational project of Mail.Ru Group: a B-tree storage behind a given C API,
then a page cache and a journal on top of it. In 2026 it was brought back to life: it builds on modern compilers,
the bugs found by review and sanitizers are fixed, and the unfinished parts
(deletion and recovery from the journal) are completed and covered by tests.

## Features

- B-tree with keys and values of variable length stored right in the nodes
  (not a B+-tree): one page read per level on lookup.
- Insertion with proactive splits on the way down (one pass, CLRS style).
- Deletion: lazy, without node merging; see [Deletion](#deletion).
- Free-space bitmap for page allocation, pages are reused after deletes.
- LRU page cache with an open-addressing hash table and write-back of dirty pages.
- Write-ahead journal with checksummed records, checkpoints and replay on open.
- Tests against a reference model, including crash recovery; ASan/UBSan clean.

## Quick start

```bash
make                # build/libmydb.a and build/mydb_demo
./build/mydb_demo   # creates demo.db and demo.db.log
make test           # unit and integration tests
make SAN=1 test     # the same with AddressSanitizer + UndefinedBehaviorSanitizer
```

A POSIX system and a C99 compiler (CI runs gcc and clang on Ubuntu).
Run `make clean` before switching between `SAN=1` and a regular build.

## API

```c
#include "mydb.h"   /* include/, link with build/libmydb.a */

sDBC conf = { .db_size = 64 << 20, .page_size = 4096, .cache_size = 4 << 20 }; /* 0 = default */
sDB *db = dbcreate ("data.db", &conf);    /* or dbopen ("data.db") for an existing one */

db_insert (db, "key", 3, "value", 5);      /* insert or update */

void  *val;  size_t len;
if ( db_select (db, "key", 3, &val, &len) == 0 )  /* 0 - found */
{ /* use val[0..len) */ free (val); }     /* the value is malloc'ed */

db_delete (db, "key", 3);                  /* -1 if there is no such key */
db_flush  (db);                            /* checkpoint: pages to disk, journal truncated */
db_close  (db);                            /* flush + free */
```

The same operations are available as function pointers of `struct DB`
(`db->insert`, `db->select`, `db->delete`, `db->sync`, `db->close`) with `struct DBT { void *data; size_t size; }` arguments,
as required by the course API ([`include/mydb.h`](include/mydb.h), documented with doxygen comments).

| Function | Returns |
|---|---|
| `dbcreate (file, conf)` | a new database (the file is truncated), `NULL` on error |
| `dbopen (file)` | an existing database after recovery from the journal, `NULL` on error |
| `db_insert`, `db_delete`, `db_select`, `db_flush`, `db_close` | `0` on success, `-1` on error / key not found |
| `db_max_kv_size (db)` | the largest key + value size for this page size |
| `db_check (db)` | number of keys after checking the tree invariants, `-1` if broken |

Configuration (`sDBC`), a zero field takes the default:

| Field | Default | Limits |
|---|---|---|
| `db_size` | 512 MB | at least 3 pages, below 4 GB |
| `page_size` | 4 KB | at least 512 bytes |
| `cache_size` | 16 MB | at least 128 pages |

## Repository layout

| Path | Contents |
|---|---|
| [`include/`](include) | the public API: `mydb.h` (and `stdafx.h` with the common includes) |
| [`src/`](src) | the implementation and the internal headers |
| [`examples/demo.c`](examples/demo.c) | a small example of the API |
| [`tests/`](tests) | unit tests of the modules and integration tests against a model |
| [`bench/`](bench) | throughput benchmark |
| `build/` | build output (not in git) |

## Architecture

```mermaid
flowchart TD
    API["src/mydb.c: db_insert / db_select / db_delete"] --> M["src/mydb_methods.c: create, open, close, flush"]
    M --> LOG["src/mydb_log.c: write-ahead journal, recovery"]
    M --> BT["src/mydb_block.c, src/mydb_block_delete.c: B-tree algorithms"]
    BT --> LOW["src/mydb_block_low.c: entries inside a page"]
    BT --> C["src/mydb_cache.c: LRU page cache"]
    BT --> TB["src/mydb_techb.c: free-page bitmap"]
    C --> F[("data.db")]
    TB --> F
    LOG --> J[("data.db.log")]
```

### File layout

```mermaid
flowchart LR
    H["header<br/>magic, version, sizes,<br/>root page, page count"] --> B["bitmap pages<br/>1 bit per data page"] --> D["data pages<br/>B-tree nodes"]
```

The header (36 bytes) keeps the page size, the number of bitmap and data pages and the root page number.
The bitmap takes `ceil(pages / (8 * page_size))` pages, one bit marks a used data page.
Data pages are addressed by their index, child pointers are 32-bit page indices.

### Page layout

Every data page starts with a 20-byte header: node type (leaf / inner), number of keys,
own page index, free bytes and the leftmost child pointer. Then entries follow, sorted by key:

```text
| type | nkeys | offset | free | ptr0 | ksz | vsz | key | value | ptr1 | ksz | vsz | key | value | ptr2 | ... free ... |
                                     \_________ entry 1 _____________/ \_________ entry 2 ___________/
```

`ptr0` is the child with keys less than key 1, `ptrN` is the child between key N and key N+1 (unused in leaves).
Keys are compared with `memcmp`, then by length. Integer fields inside a page are unaligned
and are accessed with `memcpy`.

An entry (12 bytes + key + value) may take at most 1/8 of the page payload
(497 bytes of key + value for 4 KB pages), and a page counts as *full* when less than 1/4 of it is free.
Together these guarantee that a non-full page always has room for a key coming up
from a split child and for growing one of its entries.

### Insertion

```mermaid
flowchart TD
    S([insert key, value]) --> J[append the operation to the journal]
    J --> R{root full?}
    R -- yes --> NR[new root above, split the old one]
    R -- no --> X
    NR --> X[x = root]
    X --> F{key in x?}
    F -- yes --> U[update the value in place]
    F -- no --> L{x is a leaf?}
    L -- yes --> I[insert the entry into x]
    L -- no --> C[y = child of x for the key]
    C --> CF{y full?}
    CF -- yes --> SP[split y, the median goes up into x] --> F
    CF -- no --> D[x = y] --> F
```

A full page is split by bytes, not by the number of keys: the left half keeps entries
until it holds about a half of the used space, the next entry becomes the median.

### Deletion

Deletion follows the same rule "every page on the path is non-full" and never merges pages:

- a key in a leaf is removed, the leaf may become underfull or empty;
- a key in an inner page is replaced by its predecessor
  (the last key of the deepest non-empty page on the right spine of the left subtree),
  or by its successor if the left subtree is empty;
- if both neighbouring subtrees are empty, the key is removed with its right subtree and the pages are freed;
- an inner root without keys is dropped and the tree becomes lower.

All leaves stay at the same depth, but the pages are not rebalanced:
space is reused by later inserts, the file never shrinks.

### Page cache

```mermaid
flowchart LR
    G["block_create (page)"] --> HT{"in the hash table?"}
    HT -- yes --> MRU[move to the head of the LRU list]
    HT -- no --> FULL{cache full?}
    FULL -- no --> RD[read into a free slot]
    FULL -- yes --> DT{LRU tail dirty?}
    DT -- yes --> SN["write all dirty pages,<br/>bitmap and header"] --> EV
    DT -- no --> EV[evict the tail] --> RD
```

Pages are looked up by number in an open-addressing hash table whose cells are linked
into the LRU list. Modified pages are only marked dirty and are written on eviction or checkpoint.

### Journal and recovery

Each `insert` / `delete` appends a record to `<file>.log` *before* changing the tree:

```text
| type u32 | key size u32 | value size u32 | LSN u64 | key | value | FNV-1a checksum u32 |
```

```mermaid
sequenceDiagram
    participant App
    participant DB as mydb
    participant Log as data.db.log
    participant File as data.db
    App->>DB: insert / delete
    DB->>Log: append the record (write)
    DB->>DB: change pages in the cache
    App->>DB: db_flush / db_close
    DB->>File: all dirty pages, bitmap, header, fsync
    DB->>Log: truncate, write CHECKPOINT, fsync
    Note over App,File: crash
    App->>DB: dbopen
    DB->>Log: replay records after the last checkpoint
    DB->>File: checkpoint
```

Why the replay is correct: dirty pages never reach the disk one by one.
When the cache has to evict a dirty page, it writes *all* dirty pages, the bitmap and the header together,
at a moment when the tree in memory is valid (between page fetches of an operation).
So the file always holds a consistent tree that reflects a prefix of the journal,
and insert/delete are idempotent: repeating the operations from the last checkpoint gives the same state.
A record torn by a crash is detected by its checksum and ignored.

## Tests

[`tests/test_mydb.c`](tests/test_mydb.c) checks every operation against a sorted-array model
and verifies the tree invariants with `db_check`:

- basic operations, updates, empty values, invalid arguments, foreign and missing files;
- 20–30 thousand random inserts / updates / deletes with the default cache and with the smallest
  cache (many evictions), with and without reopening, with 512-byte and 4 KB pages;
- deleting all keys and inserting them again (page reuse);
- filling the file completely;
- crash recovery: a child process runs operations and exits without closing the database,
  the parent reopens it and compares with the model, also with a torn journal tail.

`MYDB_CRASH_ROUNDS=20 make test` runs more crash rounds.

## Performance

`make bench` (source: [`bench/bench_mydb.c`](bench/bench_mydb.c)) measures the throughput with 16-byte keys
and 32-byte values. 50 000 keys, gcc 11 `-O2`, WSL2 on a desktop CPU, one run:

| configuration | insert, sequential | insert, random | select, present | select, absent | update | delete, random |
|---|---:|---:|---:|---:|---:|---:|
| page 4K, cache 16M (the tree fits) | 319K ops/s | 186K | 594K | 634K | 302K | 405K |
| page 4K, cache 1M (does not fit) | 221K | 22K | 328K | 189K | 13K | 13K |
| page 16K, cache 16M | 158K | 228K | 275K | 230K | 236K | 250K |

A checkpoint (`db_flush`) takes 25–40 ms, mostly `fsync`.
When the cache is smaller than the tree, random modifications are about 10 times slower:
evicting a dirty page writes *all* dirty pages and calls `fsync` (see [Journal and recovery](#journal-and-recovery)) —
the price of the simple crash-consistency scheme. CI runs the benchmark on every push and shows the table in the job summary.

## Limitations

- Single-threaded, one process at a time; no file locking.
- Durability against a process crash, not against a power loss:
  journal records are written without `fsync` (only checkpoints are synced),
  and a crash in the middle of writing the pages can leave the file inconsistent (no double-write buffer).
- Key + value size is limited by the page size (1/8 of a page); empty keys are not allowed.
- No range scans or iterators; pages are not merged after deletes, the file does not shrink.
- Integers are stored in the native byte order (the files are not portable between endiannesses).
- Fatal I/O errors inside the cache terminate the process (`exit`), as in the original code.

## License

[GPL-2.0](LICENSE)
