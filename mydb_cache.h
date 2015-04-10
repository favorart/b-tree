#include "stdafx.h"
#include "block.h"
#include "mydb.h"

#ifndef _MYDB_CACHE_H_
#define _MYDB_CACHE_H_
//-------------------------------------------------------------------------------------------------------------
typedef uint32_t hash_t;

typedef struct page_cache sPgCache;
struct page_cache
{
  uint32_t n_pages;
  /* double linked list of pages */
  sBlock *lru;
  /* quickly look up a page by page id — a hash */
  hash_t all_pages;
};
//-------------------------------------------------------------------------------------------------------------
#endif // _MYDB_CACHE_H_
