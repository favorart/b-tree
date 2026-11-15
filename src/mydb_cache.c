#include "stdafx.h"
#include "mydb_block.h"
#include "mydb_cache.h"
#include "mydb_low.h"
#include "mydb_trace.h"

//-------------------------------------------------------------------------------------------------------------
/* A hash table cell, linked into the LRU list */
struct Cache_CyclicHashTable_BuildInList
{
    sBlock *block;   // ptr to a cell in the blocks array
    uint32_t offset; // = 0 for an empty cell
    //----------------
    sCchHL *next;
    sCchHL *prev;
    //----------------
};
//-------------------------------------------------------------------------------------------------------------
#ifdef CACHE_HASH_IDEAL
#define CACHE_HASH_COEF    11000
#define cache_hash(offset) (offset)
#else
#define CACHE_HASH_COEF 1.5
static hash_t cache_hash(IN uint32_t offset)
{
    hash_t hash = 0U;
    unsigned char *pc = (unsigned char *)&offset;

    for (size_t i = 0; i < sizeof(offset); ++i, ++pc)
    {
        hash += *pc;
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);

    return hash;
}
#endif

static hash_t cache_hash_get(IN sDB *db, IN uint32_t offset)
{
    sCache *Cch = db->cache_;
    //-----------------------------------------------
    /* 0 marks an empty cell, so offsets are stored + 1 */
    ++offset;

    hash_t h = cache_hash(offset) % Cch->szHash_;
    while (Cch->blHash_[h].offset)
    {
        /* linear probing until found or an empty cell */
        if (Cch->blHash_[h].offset == offset)
            return h;
        h = (h + 1U) % Cch->szHash_;
    }
    //-----------------------------------------------
    return Cch->szHash_;
}
/* linear probing: put into the first empty cell after the home one */
static hash_t cache_hash_set(IN sDB *db, IN uint32_t offset)
{
    sCache *Cch = db->cache_;
    //-----------------------------------------------
    /* 0 marks an empty cell, so offsets are stored + 1 */
    ++offset;

    hash_t h = cache_hash(offset) % Cch->szHash_;
    for (uint_t probes = 0U; Cch->blHash_[h].offset; ++probes)
    {
        if (probes == Cch->szHash_)
            return Cch->szHash_; /* no empty cell */
        h = (h + 1U) % Cch->szHash_;
    }
    Cch->blHash_[h].offset = offset;
    return h;
}
/* move the cell (with its place in the LRU list) */
static void cache_cell_move(IN sCache *Cch, IN hash_t to, IN hash_t from)
{
    sCchHL *dst = &Cch->blHash_[to];
    *dst = Cch->blHash_[from];
    memset(&Cch->blHash_[from], 0, sizeof(*dst));

    if (dst->next)
        dst->next->prev = dst;
    else
        Cch->LRULast = dst;

    if (dst->prev)
        dst->prev->next = dst;
    else
        Cch->LRUHead = dst;
}
/*  Delete the cell (already unlinked from the LRU list) with the backward shift
 *  (Knuth, Algorithm R): the following cells of the probe sequence, whose home
 *  is not between the hole and themselves, are moved into the hole.
 */
static void cache_hash_pop(IN sDB *db, IN hash_t hole)
{
    sCache *Cch = db->cache_;
    //-----------------------------------------------
    memset(&Cch->blHash_[hole], 0, sizeof(*Cch->blHash_));

    for (hash_t j = (hole + 1U) % Cch->szHash_; Cch->blHash_[j].offset; j = (j + 1U) % Cch->szHash_)
    {
        hash_t home = cache_hash(Cch->blHash_[j].offset) % Cch->szHash_;
        /* is the home cyclically in (hole, j]? then the cell stays reachable */
        bool stays = (hole <= j) ? (hole < home && home <= j) : (hole < home || home <= j);
        if (stays)
            continue;

        cache_cell_move(Cch, hole, j);
        hole = j;
    }
}
//-------------------------------------------------------------------------------------------------------------
bool mydb_cache_init(IN sDB *db)
{
    bool was_fail = false;
    //-----------------------------------------------
    db->cache_ = (sCache *)calloc(1U, sizeof(*db->cache_));
    if (db->cache_)
    {
        sCache *Cch = db->cache_;
        //----------------------------------------------
        memset(Cch, 0, sizeof(*Cch));

        Cch->m_size_ = db->head_.cache_size_;
        Cch->n_pgs = 0;
        Cch->all_pgs = db->head_.cache_size_ / db->head_.page_size_;
        if (Cch->all_pgs < MYDB_MIN_CACHE_PAGES)
            Cch->all_pgs = MYDB_MIN_CACHE_PAGES;
        Cch->sz_pg = db->head_.page_size_;
        Cch->szHash_ = (uint_t)((double)Cch->all_pgs * CACHE_HASH_COEF);
        //-----------------------------------------------
        Cch->blocks_ = (sBlock *)calloc(Cch->all_pgs, sizeof(*Cch->blocks_));
        Cch->blHash_ = (sCchHL *)calloc(Cch->szHash_, sizeof(*Cch->blHash_));
        Cch->memory_ = (uchar_t *)malloc(Cch->all_pgs * Cch->sz_pg * sizeof(uchar_t));

        if (!Cch->blocks_ || !Cch->memory_ || !Cch->blHash_)
        {
            fprintf(stderr, "cache init: %s\n", strerror(errno));
            was_fail = true;
            mydb_cache_free(db);
        }
        else
        {
            //-----------------------------------------------
            Cch->LRUHead = Cch->blHash_;
            Cch->LRULast = Cch->blHash_;
        }
    }
    else
    {
        fprintf(stderr, "cache init: %s\n", strerror(errno));
        was_fail = true;
    }
    //-----------------------------------------------
    return was_fail;
}
bool mydb_cache_push(IN sDB *db, IN uint32_t offset, OUT sBlock **block)
{
    sCache *Cch = db->cache_;
    bool in_hash = false;

    sCchHL *hl = NULL;
    //-----------------------------------------------
    hash_t h = cache_hash_get(db, offset);
    if (h >= Cch->szHash_)
    {
        sBlock *hl_block = NULL;
        uchar_t *hl_memory = NULL;
        //-----------------------------------------------
        /* the block is not in the cache */
        if (Cch->n_pgs == Cch->all_pgs)
        {
            if (Cch->LRULast->block == db->root_)
            {
                Cch->LRULast->next = Cch->LRUHead;
                Cch->LRUHead->prev = Cch->LRULast;

                Cch->LRULast = Cch->LRULast->prev;
                Cch->LRUHead = Cch->LRUHead->prev;

                Cch->LRUHead->prev = NULL;
                Cch->LRULast->next = NULL;
            }
            //-----------------------------------------------
            /*  The cache is full.
       *  A dirty block must be written to disk before eviction.
       *  Writing a single page would leave on disk a mix of old and new
       *  pages, that the journal cannot repair. So all dirty pages, the
       *  bitmap and the header are written together: the disk always holds
       *  a consistent tree, and replaying the journal from the last
       *  checkpoint over it gives the right result (operations are idempotent).
       */
            TRACE(MYDB_TRACE_PAGES, "cache miss page %u: evict page %u%s", (unsigned)offset,
                  (unsigned)Cch->LRULast->offset,
                  Cch->LRULast->block->dirty_ ? " (dirty: write all dirty pages)" : "");
            if (Cch->LRULast->block->dirty_ && mydb_sync_pages(db))
            {
                mydb_errno = MYDB_ERR_BWRITE;
                fprintf(stderr, "cache push%s\n", strmyerror(mydb_errno));
                exit(EXIT_FAILURE); // !!!
            }
            //-----------------------------------------------
            /* keep the pointer to the freed block */
            hl_block = Cch->LRULast->block;

            /* evict it from the cache */
            h = (Cch->LRULast - Cch->blHash_);
            //---LRU List------------------------------------
            Cch->LRULast = Cch->LRULast->prev;
            Cch->LRULast->next = NULL;
            //-----------------------------------------------
            cache_hash_pop(db, h);
            //-----------------------------------------------
        }
        //-----------------------------------------------
        /* there is a free cell now: add the block */
        h = cache_hash_set(db, offset); // can be error
        if (h == Cch->szHash_)
        {
            mydb_errno = MYDB_ERR_CCHSET;
            fprintf(stderr, "cache set%s\n", strmyerror(mydb_errno));
            exit(EXIT_FAILURE); // !!!
        }
        //-----------------------------------------------
        hl = &Cch->blHash_[h];
        if (Cch->n_pgs == Cch->all_pgs)
        {
            hl->block = hl_block;
            hl_memory = hl_block->memory_;
        }
        else
        {
            TRACE(MYDB_TRACE_PAGES, "cache miss page %u: free slot %u of %u", (unsigned)offset,
                  (unsigned)Cch->n_pgs, (unsigned)Cch->all_pgs);
            hl->block = &Cch->blocks_[Cch->n_pgs];
            hl_memory = (Cch->memory_ + Cch->n_pgs * Cch->sz_pg);
            ++Cch->n_pgs;
        }
        //-----------------------------------------------
        // block_free () <--
        memset(hl->block, 0, sizeof(*hl->block));
        memset(hl_memory, 0, Cch->sz_pg);
        hl->block->memory_ = hl_memory;

        //---LRU List------------------------------------
        hl->prev = NULL;
        /* LRUHead points to a cell of a cached page unless the cache was empty
         * (testing LRUHead->offset fails when the first page lands in cell 0) */
        if (Cch->LRUHead != hl && Cch->LRUHead->offset)
        {
            /* the cache is not empty */
            hl->next = Cch->LRUHead;
            Cch->LRUHead->prev = hl;
        }
        else
        {
            Cch->LRULast = hl;
            hl->next = NULL;
        }
        Cch->LRUHead = hl;
        //-----------------------------------------------
    }
    else
    {
        //-----------------------------------------------
        /* the block is in the cache */
        hl = &Cch->blHash_[h];
        in_hash = true;

        /* If a block is found, put it first in the LRU-list, then return */
        //---LRU List------------------------------------
        if (Cch->LRUHead != &Cch->blHash_[h])
        {
            if (Cch->blHash_[h].next)
                Cch->blHash_[h].next->prev = Cch->blHash_[h].prev;
            else
                Cch->LRULast = Cch->LRULast->prev;

            if (Cch->blHash_[h].prev)
                Cch->blHash_[h].prev->next = Cch->blHash_[h].next;

            Cch->blHash_[h].prev = NULL;
            Cch->blHash_[h].next = Cch->LRUHead;

            Cch->LRUHead->prev = &Cch->blHash_[h];
            Cch->LRUHead = &Cch->blHash_[h];
        }
        //-----------------------------------------------
    }
    //-----------------------------------------------
    (*block) = hl->block;
    (*block)->offset_ = offset;

    //-----------------------------------------------
    //-----------------------------------------------
    return in_hash;
}
bool mydb_cache_sync(IN sDB *db)
{
    sCache *Cch = db->cache_;
    //-----------------------------------------------
    /* every used cell of the blocks array is a cached page (the LRU tail too) */
    uint_t dirty = 0U;
    for (uint_t i = 0U; i < Cch->n_pgs; ++i)
        dirty += Cch->blocks_[i].dirty_;
    TRACE(MYDB_TRACE_PAGES, "write %u dirty pages of %u cached", (unsigned)dirty, (unsigned)Cch->n_pgs);
    for (uint_t i = 0U; i < Cch->n_pgs; ++i)
    {
        if (DONE != block_dump(&Cch->blocks_[i]))
        {
            mydb_errno = MYDB_ERR_BWRITE;
            fprintf(stderr, "cache sync%s\n", strmyerror(mydb_errno));
            return true;
        }
    }
    return false;
}
void mydb_cache_free(IN sDB *db)
{
    if (db->cache_)
    {
        free(db->cache_->blocks_);
        free(db->cache_->memory_);
        free(db->cache_->blHash_);
    }
    //-----------------------------------------------
    free(db->cache_);
    db->cache_ = NULL;
}
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
