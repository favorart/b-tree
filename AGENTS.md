# Notes for contributors and coding agents

- Build: `make`; tests: `make test`; sanitizers: `make clean && make SAN=1 test`.
  CI builds with `-Werror` on gcc and clang (Linux, with and without ASan/UBSan) and Apple Clang (macOS): keep the build warning-free.
- C99 + POSIX. Format with `make format` (clang-format, `.clang-format`); keep the `IN`/`OUT` markers
  and the `s`/`e` type prefixes. Comments are in English; the public API in `include/mydb.h` has doxygen comments.
- Layout: `include/` public API, `src/` implementation, `examples/`, `tests/`; everything builds into `build/`.
- Entries inside a page are unaligned: read and write the 32-bit fields with `mydb_rd32` / `mydb_wr32`.
- A `sBlock *` from `block_create` lives in the page cache and is valid only during one operation.
- Crash safety rule: between two `block_create` calls the tree in memory must be valid,
  because a cache eviction may write all dirty pages at that moment (see `mydb_cache_push` in `src/mydb_cache.c`).
  Make the changes of one logical step (e.g. a key replacement in two pages) without fetching pages in between.
- On-disk format changes need `MYDB_VERSION` bumped in `include/mydb.h` and the README updated.
- Debug with the trace (`MYDB_TRACE=1..4`, `src/mydb_trace.h`), not with `printf`: add a `TRACE ()` point for a new action.
- Every behaviour change needs a test: `tests/test_units.c` for a module, `tests/test_mydb.c` through the API; `db_check` validates the tree.
- Releases are automatic: every push to `master` that passes CI is tagged `v0.0.N` and released (`.github/workflows/ci.yml`).
- Commit messages and PR titles follow Conventional Commits: `fix:`, `feat:`, `refactor:`, `test:`, `docs:`, `ci:`, `build:`.
