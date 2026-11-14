# Notes for contributors and coding agents

- Build: `make`; tests: `make test`; sanitizers: `make clean && make SAN=1 test`.
  CI builds with `-Werror` on gcc and clang, with and without ASan/UBSan: keep the build warning-free.
- C99 + POSIX. Keep the original code style (spacing, `IN`/`OUT` markers, `s`/`e` type prefixes).
- Entries inside a page are unaligned: read and write the 32-bit fields with `mydb_rd32` / `mydb_wr32`.
- A `sBlock *` from `block_create` lives in the page cache and is valid only during one operation.
- Crash safety rule: between two `block_create` calls the tree in memory must be valid,
  because a cache eviction may write all dirty pages at that moment (see `mydb_cache_push`).
  Make the changes of one logical step (e.g. a key replacement in two pages) without fetching pages in between.
- On-disk format changes need `MYDB_VERSION` bumped in `mydb.h` and the README updated.
- Every behaviour change needs a test in `tests/test_mydb.c`; `db_check` validates the tree.
