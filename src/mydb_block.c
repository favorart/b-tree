#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_trace.h"
#include "mydb_techb.h"
#include "mydb_cache.h"

//-------------------------------------------------------------------------------------------------------------
/* DECORATIVE OPERATIONS */

eDBNT *block_type(IN sBlock *block)
{
    return &block->head_->node_type_;
}
uint_t *block_nkvs(IN sBlock *block)
{
    return &block->head_->kvs_count_;
}
uint32_t block_lptr(IN sBlock *block, IN const sDBT *key)
{
    /* | ptr | ksz | vsz | key | val | */
    return (key) ? mydb_rd32((uchar_t *)key->data - 3U * sizeof(uint32_t))
                 : mydb_rd32(block->free_ - sizeof(uint32_t));
}
uint32_t block_rptr(IN sBlock *block, IN const sDBT *key)
{
    /* | ksz | vsz | key | val | ptr | */
    return (key) ? mydb_rd32((uchar_t *)key->data + key->size +
                             mydb_rd32((uchar_t *)key->data - sizeof(uint32_t)))
                 : block->head_->pointer_0_;
}
//-------------------------------------------------------------------------------------------------------------
/*  An entry is limited to 1/8 of the block payload. Together with the "full"
 *  threshold of 1/4 it guarantees, that a non-full block always has room for
 *  a median key coming from a split child AND for growing one of its entries
 *  (update of a value or replacing a deleted key by its predecessor).
 */
uint32_t block_max_entry(IN const sDB *db)
{
    return (uint32_t)((db->head_.page_size_ - sizeof(sDBHB)) / 8U);
}
/* full:   free = (1/4 * size) */
bool block_isfull(IN const sBlock *block)
{
    return (4U * block->head_->free_size_ <= 1U * (block->size_ - sizeof(sDBHB)));
}
//-------------------------------------------------------------------------------------------------------------
/* HIGH-LEVEL OPERATIONS */

/*  Insert into the subtree of a non-full block (CLRS B-TREE-INSERT-NONFULL):
 *  every full child is split before going down into it,
 *  so the median key always has a room in the parent.
 */
eDBState block_add_nonfull(IN sBlock *block, IN const sDBT *key, IN const sDBT *value)
{
    if ((*block_type(block)) == Leaf)
    {
        TRACE(MYDB_TRACE_TREE, "page %u leaf, %u keys, %u B free: insert or update", (unsigned)block->offset_,
              (unsigned)*block_nkvs(block), (unsigned)block->head_->free_size_);
        if (DONE != block_insert(block, key, value, NULL, MYDB_INVALIDPTR))
            return FAIL;
        return block_write(block);
    }
    //-----------------------------------------
    /* Passing down through the tree */
    for (;;)
    {
        sDBT iter = {0};
        sDBT *k = block_key_next(block, &iter, NULL);
        while (k && key_compare(k, key) < 0)
        {
            k = block_key_next(block, k, NULL);
        }
        //-----------------------------------------
        /* the key lives in this inner block - update the value in place */
        if (k && !key_compare(k, key))
        {
            TRACE(MYDB_TRACE_TREE, "page %u inner: the key is here, update in place",
                  (unsigned)block->offset_);
            if (DONE != block_change(block, key, value, k, MYDB_INVALIDPTR))
                return FAIL;
            return block_write(block);
        }
        //-----------------------------------------
        TRACE(MYDB_TRACE_TREE, "page %u inner, %u keys, %u B free -> child page %u", (unsigned)block->offset_,
              (unsigned)*block_nkvs(block), (unsigned)block->head_->free_size_,
              (unsigned)block_lptr(block, k));
        sBlock *ychild = block_create(block->owner_db_, block_lptr(block, k));
        if (!ychild)
            return FAIL;
        //-----------------------------------------
        /* Keep all block non-full through the path */
        if (block_isfull(ychild))
        {
            sBlock *zchild = block_create(block->owner_db_, MYDB_OFFSET2NEW);
            if (!zchild)
                return FAIL;
            /* split the block, if there is a full one */
            if (DONE != block_split_child(block, ychild, zchild))
                return FAIL;
            /* the median key went up into this block: look again */
            continue;
        }
        //-----------------------------------------
        /* insert into non-full block */
        return block_add_nonfull(ychild, key, value);
    }
}
eDBState block_select_deep(IN sBlock *block, IN const sDBT *key, OUT sDBT *value)
{
    const char *error_prefix = "memory block searching";
    //-----------------------------------------
    for (;;)
    {
        sDBT iter = {0};
        sDBT *k = block_key_next(block, &iter, NULL);
        while (k && key_compare(k, key) < 0)
        {
            k = block_key_next(block, k, NULL);
        }
        //-----------------------------------------
        if (k && !key_compare(key, k))
        {
            TRACE(MYDB_TRACE_TREE, "page %u %s: found", (unsigned)block->offset_,
                  (*block_type(block) == Leaf) ? "leaf" : "inner");
            sDBT val = block_key_data(block, k);
            //-----------------------------------------
            value->data = malloc(val.size ? val.size : 1U);
            if (!value->data)
            {
                fprintf(stderr, "%s%s\n", error_prefix, strerror(errno));
                return FAIL;
            }
            value->size = val.size;
            memcpy(value->data, val.data, val.size);
            return DONE;
        }
        //-----------------------------------------
        if ((*block_type(block)) == Leaf)
        {
            TRACE(MYDB_TRACE_TREE, "page %u leaf: not found", (unsigned)block->offset_);
            value->data = NULL;
            value->size = 0;
            return FAIL;
        }
        //-----------------------------------------
        TRACE(MYDB_TRACE_TREE, "page %u inner, %u keys -> child page %u", (unsigned)block->offset_,
              (unsigned)*block_nkvs(block), (unsigned)block_lptr(block, k));
        if (!(block = block_create(block->owner_db_, block_lptr(block, k))))
            return FAIL;
    }
}
//-------------------------------------------------------------------------------------------------------------
/* ychild - full node; zchild - new free node */
eDBState block_split_child(IN sBlock *parent, IN sBlock *ychild, OUT sBlock *zchild)
{
    /*-----------------------------------------
   *    x - parent,   y,z - childs,   k - key
   *    x: .....      x: ..k..
   *        /     =>      / \
   *      ykz            y   z
   *-----------------------------------------*/
    (*block_type(zchild)) = (*block_type(ychild));
    //-----------------------------------------
    /* Calculate the number of items to delegate */
    uint_t amount_size = 0U, count = 0U, vsz = 0U;

    sDBT iter = {0};
    sDBT *k = block_key_next(ychild, &iter, &vsz);
    while (k && amount_size < (uint_t)(ychild->free_ - ychild->data_) / 2U)
    { /* Iterate through the elements, those stay in ychild */
        ++count;
        amount_size += (uint_t)(k->size + vsz + MYDB_ENTRY_OVERHEAD);
        k = block_key_next(ychild, k, &vsz);
    }
    /* A full block holds at least 6 entries (see block_max_entry) */
    if (!k)
    {
        fprintf(stderr, "memory block split%s\n", strmyerror(MYDB_ERR_FPARAM));
        return FAIL;
    }
    sDBT val = block_key_data(ychild, k);

    /* move the middle element into the parent block */
    if (DONE != block_insert(parent, k, &val, NULL, zchild->offset_))
        return FAIL;

    /* RPtr must by copied to zchild->pointer_0_, not to parent RPtr */
    zchild->head_->pointer_0_ = block_rptr(ychild, k);

    ++count;
    amount_size += (uint_t)(k->size + val.size + MYDB_ENTRY_OVERHEAD);
    //-----------------------------------------
    uint_t bytes_count = (uint_t)((ychild->free_ - ychild->data_) - amount_size);
    memcpy(zchild->data_, ychild->data_ + amount_size, bytes_count);

    (*block_nkvs(zchild)) = (*block_nkvs(ychild) - count); /* latest elements of ychild */
    (*block_nkvs(ychild)) = (count - 1U);                  /* remaining elements without the middle one */
    //-----------------------------------------
    /* ptr's have already copied with elements data */
    //-----------------------------------------
    zchild->head_->free_size_ -= bytes_count;
    zchild->free_ += bytes_count;

    /* take into consideration the inserted element */
    bytes_count += (uint_t)(k->size + val.size + MYDB_ENTRY_OVERHEAD);

    ychild->head_->free_size_ += bytes_count;
    ychild->free_ -= bytes_count;
    memset(ychild->free_, 0, bytes_count);
    //-----------------------------------------
    block_write(ychild);
    block_write(zchild);
    block_write(parent);
    //-----------------------------------------
    if (TRACE_ON(MYDB_TRACE_TREE))
    {
        char buf[MYDB_TRACE_KEYBUF];
        sDBT median = {0};
        for (sDBT it = {0}, *m = block_key_next(parent, &it, NULL); m; m = block_key_next(parent, m, NULL))
            if (block_rptr(parent, m) == zchild->offset_)
                median = *m;
        TRACE(MYDB_TRACE_TREE, "split %s page %u -> new page %u, median %s up into page %u (%u + %u keys)",
              (*block_type(ychild) == Leaf) ? "leaf" : "inner", (unsigned)ychild->offset_,
              (unsigned)zchild->offset_, mydb_trace_key(median.data, median.size, buf),
              (unsigned)parent->offset_, (unsigned)*block_nkvs(ychild), (unsigned)*block_nkvs(zchild));
    }
    return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/*  Get a block by its offset (MYDB_OFFSET2NEW - allocate a new one).
 *  Blocks live in the page cache: the pointer is valid until the block is
 *  evicted, i.e. for the duration of one db operation (see MYDB_MIN_CACHE_PAGES).
 */
sBlock *block_create(IN sDB *db, IN uint_t offset)
{
    const char *error_prefix = "memory block creation";
    const bool is_new = (offset == MYDB_OFFSET2NEW);
    //-----------------------------------------
    uint_t block_offset = offset;
    if (is_new)
    {
        block_offset = techb_get_index_of_first_free_bit(db);
        if (block_offset >= db->head_.block_count_)
        {
            mydb_errno = MYDB_ERR_NFREEB;
            fprintf(stderr, "%s%s\n", error_prefix, strmyerror(mydb_errno));
            return NULL;
        }
    }
    else if (block_offset >= db->head_.block_count_)
    {
        mydb_errno = MYDB_ERR_OFFSET;
        fprintf(stderr, "%s%s Offset is %u.\n", error_prefix, strmyerror(mydb_errno), (unsigned)offset);
        return NULL;
    }
    //-----------------------------------------
    /* look the block up in the cache before reading it from disk */
    sBlock *block = NULL;
    bool in_cache = mydb_cache_push(db, block_offset, &block);
    if (!block || !block->memory_)
    {
        mydb_errno = MYDB_ERR_CCHSET;
        fprintf(stderr, "%s%s\n", error_prefix, strmyerror(mydb_errno));
        return NULL;
    }
    //-----------------------------------------
    /* A freed block may still be in the cache: a new block is always reset */
    if (!in_cache || is_new)
    {
        block->owner_db_ = db;
        block->size_ = db->head_.page_size_;
        block->is_mem_ = true;
        block->head_ = (sDBHB *)block->memory_;
        block->offset_ = block_offset;

        if (is_new)
        {
            memset(block->memory_, 0, block->size_);
            block->head_->db_offset_ = block_offset;
            block->head_->free_size_ = block->size_ - (uint32_t)sizeof(sDBHB);
            block->head_->kvs_count_ = 0U;
            block->head_->node_type_ = Leaf;
            block->head_->pointer_0_ = MYDB_INVALIDPTR;
            block->dirty_ = true;
        }
        else if (DONE != block_read(block))
        {
            memset(block->memory_, 0, block->size_);
            mydb_errno = MYDB_ERR_FPARAM;
            fprintf(stderr, "%s%s %s\n", error_prefix, strmyerror(mydb_errno), strerror(errno));
            return NULL;
        }
    }
    //-----------------------------------------
    if (block->head_->node_type_ == Free || block->head_->db_offset_ != block_offset)
    {
        mydb_errno = MYDB_ERR_FORMAT;
        fprintf(stderr, "%s%s Block %u is not a node.\n", error_prefix, strmyerror(mydb_errno),
                (unsigned)block_offset);
        return NULL;
    }

    block->data_ = (block->memory_ + sizeof(sDBHB));
    block->free_ = (block->memory_ + (block->size_ - block->head_->free_size_));
    //-----------------------------------------
    return block;
}
void block_release(IN sDB *db, IN uint32_t offset)
{
    TRACE(MYDB_TRACE_TREE, "release page %u", (unsigned)offset);
    techb_set_bit(db, offset, false);
}
//-------------------------------------------------------------------------------------------------------------
/*  Walk the subtree and check the invariants: keys are sorted and lie
 *  between the bounds from the parent, all leaves are at the same depth,
 *  the key counters match. Returns the number of keys or -1.
 */
static long block_check_subtree(IN sDB *db, IN uint32_t offset, IN const sDBT *lo, IN const sDBT *hi,
                                IN int depth, IN OUT int *leaf_depth)
{
    sBlock *b = block_create(db, offset);
    if (!b)
        return -1;
    /* copy: deeper blocks may push this one out of the cache */
    uchar_t *mem = malloc(b->size_);
    if (!mem)
        return -1;
    memcpy(mem, b->memory_, b->size_);
    sBlock copy = *b;
    copy.memory_ = mem;
    copy.head_ = (sDBHB *)mem;
    copy.data_ = mem + (b->data_ - b->memory_);
    copy.free_ = mem + (b->free_ - b->memory_);
    b = &copy;

    long count = 0;
    bool leaf = (*block_type(b) == Leaf);
    const sDBT *prev = lo;
    sDBT prev_buf = {0}, iter = {0};
    uint32_t n = 0U;
    for (sDBT *k = block_key_next(b, &iter, NULL); k; k = block_key_next(b, k, NULL), ++n)
    {
        sDBT cur = *k;
        if (prev && key_compare(prev, &cur) >= 0)
        {
            fprintf(stderr, "check: block %u: keys are not sorted\n", (unsigned)offset);
            count = -1;
            break;
        }
        if (!leaf)
        {
            long c = block_check_subtree(db, block_lptr(b, &cur), prev, &cur, depth + 1, leaf_depth);
            if (c < 0)
            {
                count = -1;
                break;
            }
            count += c;
        }
        ++count;
        prev_buf = cur; /* points into the copy, stays valid */
        prev = &prev_buf;
        iter = cur;
        k = &iter;
    }
    if (count >= 0 && n != *block_nkvs(b))
    {
        fprintf(stderr, "check: block %u: nkvs %u != %u\n", (unsigned)offset, (unsigned)*block_nkvs(b),
                (unsigned)n);
        count = -1;
    }
    if (count >= 0 && hi && n && key_compare(prev, hi) >= 0)
    {
        fprintf(stderr, "check: block %u: key above the upper bound\n", (unsigned)offset);
        count = -1;
    }
    if (count >= 0 && !leaf)
    {
        /* the very right child; an inner block may have no keys and this single child */
        long c = block_check_subtree(db, block_lptr(b, NULL), prev, hi, depth + 1, leaf_depth);
        count = (c < 0) ? -1 : count + c;
    }
    if (count >= 0 && leaf)
    {
        if (*leaf_depth < 0)
            *leaf_depth = depth;
        else if (*leaf_depth != depth)
        {
            fprintf(stderr, "check: leaf %u at depth %d, expected %d\n", (unsigned)offset, depth,
                    *leaf_depth);
            count = -1;
        }
    }
    free(mem);
    return count;
}
long block_check_tree(IN sDB *db)
{
    int leaf_depth = -1;
    return block_check_subtree(db, db->head_.offset2root_, NULL, NULL, 0, &leaf_depth);
}
//-------------------------------------------------------------------------------------------------------------
/* walk the subtree and sum up the page statistics */
static int block_stats_subtree(IN sDB *db, IN uint32_t offset, IN unsigned depth, IN OUT sDBStat *st)
{
    sBlock *b = block_create(db, offset);
    if (!b)
        return -1;
    /* copy: deeper blocks may push this one out of the cache */
    uchar_t *mem = malloc(b->size_);
    if (!mem)
        return -1;
    memcpy(mem, b->memory_, b->size_);
    sBlock copy = *b;
    copy.memory_ = mem;
    copy.head_ = (sDBHB *)mem;
    copy.data_ = mem + (b->data_ - b->memory_);
    copy.free_ = mem + (b->free_ - b->memory_);
    b = &copy;
    //-----------------------------------------
    bool leaf = (*block_type(b) == Leaf);
    uint32_t nkvs = *block_nkvs(b);
    if (depth > st->height)
        st->height = depth;
    st->keys += nkvs;
    st->empty_pages += (nkvs == 0U);
    st->used_bytes += b->size_ - (uint32_t)sizeof(sDBHB) - b->head_->free_size_;
    st->capacity_bytes += b->size_ - (uint32_t)sizeof(sDBHB);
    if (leaf)
        ++st->leaf_pages;
    else
        ++st->inner_pages;
    //-----------------------------------------
    int rc = 0;
    if (!leaf)
    {
        sDBT iter = {0};
        for (sDBT *k = block_key_next(b, &iter, NULL); k && !rc; k = block_key_next(b, k, NULL))
            rc = block_stats_subtree(db, block_lptr(b, k), depth + 1U, st);
        if (!rc)
            rc = block_stats_subtree(db, block_lptr(b, NULL), depth + 1U, st);
    }
    free(mem);
    return rc;
}
int block_stats(IN sDB *db, OUT sDBStat *st)
{
    memset(st, 0, sizeof(*st));
    st->file_pages = db->head_.nodes_count_;
    return block_stats_subtree(db, db->head_.offset2root_, 1U, st);
}
//-------------------------------------------------------------------------------------------------------------
