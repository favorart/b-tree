#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"
#include "mydb_trace.h"

//-------------------------------------------------------------------------------------------------------------
/*  DELETION
 *
 *  Keys and values have variable length, so the classic CLRS deletion with
 *  "at least t-1 keys in a node" does not apply directly. A lazy variant is used:
 *
 *  - The path from the root is kept non-full exactly as in insertion: a full
 *    child is split before going down. A non-full block has room to accept a
 *    median key and to grow one of its entries (see block_max_entry).
 *  - A key in a leaf is just removed; leaves may become underfull or empty.
 *  - A key in an inner block is replaced by its predecessor: the last key of
 *    the deepest non-empty block on the right spine of the left subtree
 *    (the subtree to the right of that key is empty). If the left subtree is
 *    empty, the successor from the right subtree is taken symmetrically.
 *  - If both neighbour subtrees are empty, the key and its right subtree are removed.
 *
 *  Inner blocks may lose all keys and keep a single child (pointer_0_), so all
 *  leaves stay at the same depth; an empty inner root is dropped (the tree
 *  becomes lower). Nodes are never merged (no rebalancing).
 *
 *  Crash safety: dirty pages reach the disk only all together, when the cache
 *  evicts a dirty page inside block_create (). So between two block_create ()
 *  calls the tree must be valid. Splits keep it valid; the two changes of a
 *  replacement (the key in x and the neighbour in its block) are made one
 *  right after another, with both blocks already in the cache.
 */
//-------------------------------------------------------------------------------------------------------------
/*  THE CLASSIC CLRS DELETION (the 2015 design, kept for reference: not implemented,
 *  the deletion above is lazy). x - parent, y (lch) and z (rch) - children around key.
 *
 *  1. key is in an inner x, y has enough keys: find k - the predecessor of key
 *     in the subtree rooted at y, recursively delete k and replace key in x by k
 *     (k is found in one pass down).
 *
 *       x: ..key..      x: ..(key<-k)..
 *           /   \   =>       /   \
 *          y     z       k<-y     z
 *
 *  2. symmetrically, look at the child z of x, that follows the key in x:
 *     z has enough keys - find k, the key following key in the subtree rooted at z,
 *     recursively delete k and replace key in x by k.
 *
 *  3. both y and z have not enough keys - merge them with key:
 *
 *       x : ..k..      x : .....
 *            / \    =>      /
 *           y   z         ykz
 *
 *  On the way down to a child without enough keys (a key not in x):
 *
 *  rotate left (borrow from the right sibling):
 *
 *       par: ..key..      par: ..(key<-k)..
 *             /   \    =>       /        \
 *          lch     rch       lch<-key  k<-rch
 *
 *  rotate right (borrow from the left sibling):
 *
 *       par: ..key..      par: ..(k->key)..
 *             /   \    =>       /        \
 *          lch     rch       lch->k  key->rch
 *
 *  merge, when both siblings have not enough keys:
 *
 *       par: ..k..        par: .....
 *             / \      =>       /
 *          lch   rch           (lch)k(rch)
 *
 *  One pass down without recursion:
 *
 *    while ( *block_type (parent) != Leaf )
 *    {
 *      if ( block_select_data (parent, key) && block_enough (rchild) )
 *      {
 *        block_recursive_descent (parent, lchild, rchild); // EXCESS
 *        parent = rchild;
 *      }
 *      else if ( block_enough (lchild) ) // EXCESS
 *      {
 *        block_recursive_descent (parent, lchild, rchild);
 *        parent = lchild;
 *      }
 *      else
 *      {
 *        block_merge (parent, lchild, rchild); // ROTATE RIGHT if choosen first case
 *        parent = lchild;
 *      }
 *    }
 *
 *  Rotations and merges change three pages per step and need a minimal fill of a page;
 *  with variable-length entries "enough" has to be measured in bytes, not in keys.
 */
//-------------------------------------------------------------------------------------------------------------
/* first key in the block that is greater or equal to the given one */
static sDBT *block_find_ge(IN sBlock *block, IN const sDBT *key, OUT sDBT *iter)
{
    memset(iter, 0, sizeof(*iter));
    sDBT *k = block_key_next(block, iter, NULL);
    while (k && key_compare(k, key) < 0)
    {
        k = block_key_next(block, k, NULL);
    }
    return k;
}
/* the last (want_last) or the first key of a non-empty block */
static sDBT block_edge_key(IN sBlock *block, IN bool want_last)
{
    sDBT iter = {0}, edge;
    sDBT *k = block_key_next(block, &iter, NULL);
    edge = *k;
    if (want_last)
        while ((k = block_key_next(block, k, NULL)))
            edge = *k;
    return edge;
}
/* split the child, if it is full; returns FAIL on error, *split = true when splitted */
static eDBState split_if_full(IN sDB *db, IN uint32_t parent_off, IN uint32_t child_off, OUT bool *split)
{
    *split = false;
    sBlock *child = block_create(db, child_off);
    if (!child)
        return FAIL;
    if (!block_isfull(child))
        return DONE;

    sBlock *parent = block_create(db, parent_off);
    sBlock *sibling = block_create(db, MYDB_OFFSET2NEW);
    if (!parent || !sibling || !(child = block_create(db, child_off)) ||
        DONE != block_split_child(parent, child, sibling, NULL))
        return FAIL;

    *split = true;
    return DONE;
}
/*  Go down the right (want_max) or the left spine of the subtree under x, splitting full blocks.
 *  Returns -1 on error, 1 if a split was made (the caller starts again), 0 when the spine is ready:
 *  *holder is the deepest block of the spine with keys (MYDB_INVALIDPTR - the subtree is empty).
 */
static int prepare_spine(IN sDB *db, IN uint32_t xoff, IN uint32_t top, IN bool want_max,
                         OUT uint32_t *holder)
{
    uint32_t parent_off = xoff, off = top;
    *holder = MYDB_INVALIDPTR;
    for (;;)
    {
        bool split = false;
        if (DONE != split_if_full(db, parent_off, off, &split))
            return -1;
        if (split)
            return 1;

        sBlock *b = block_create(db, off);
        if (!b)
            return -1;
        if (*block_nkvs(b))
            *holder = off;
        if (*block_type(b) == Leaf)
            return 0;

        parent_off = off;
        off = want_max ? block_lptr(b, NULL) : block_rptr(b, NULL);
    }
}
/* free the blocks of an empty subtree: a chain of inner blocks without keys and an empty leaf */
static void release_empty_subtree(IN sDB *db, IN uint32_t off)
{
    while (off != MYDB_INVALIDPTR)
    {
        sBlock *b = block_create(db, off);
        if (!b)
            return;
        uint32_t next = (*block_type(b) == Pass) ? b->head_->pointer_0_ : MYDB_INVALIDPTR;
        block_release(db, off);
        off = next;
    }
}
//-------------------------------------------------------------------------------------------------------------
/* replace the key in x by the edge key of the holder block; both blocks are valid now */
static eDBState replace_by_neighbour(IN sDB *db, IN uint32_t xoff, IN const sDBT *key, IN uint32_t holder,
                                     IN bool by_successor)
{
    sBlock *h = block_create(db, holder);
    sBlock *x = block_create(db, xoff);
    if (!h || !x || !(h = block_create(db, holder)))
        return FAIL;
    //-----------------------------------------
    sDBT iter, *k = block_find_ge(x, key, &iter);
    if (!k || key_compare(k, key))
        return FAIL;

    sDBT edge = block_edge_key(h, !by_successor);
    sDBT val = block_key_data(h, &edge);
    /* the empty subtree on the far side of the neighbour */
    uint32_t side = MYDB_INVALIDPTR;
    if (*block_type(h) == Pass)
        side = by_successor ? block_lptr(h, &edge) : block_rptr(h, &edge);

    /* copy: block_change () moves the memory of x, not of h, but be safe */
    void *buf = malloc(edge.size + val.size + 1U);
    if (!buf)
        return FAIL;
    memcpy(buf, edge.data, edge.size);
    memcpy((uchar_t *)buf + edge.size, val.data, val.size);
    sDBT nkey = {buf, edge.size}, nval = {(uchar_t *)buf + edge.size, val.size};
    //-----------------------------------------
    /*   x: ..key..      x: ..(key<-k)..
   *       /   \   =>       /   \
   *      y     z       k<-y     z
   */
    if (TRACE_ON(MYDB_TRACE_TREE))
    {
        char b1[MYDB_TRACE_KEYBUF], b2[MYDB_TRACE_KEYBUF];
        TRACE(MYDB_TRACE_TREE, "page %u: replace %s by its %s %s from page %u", (unsigned)xoff,
              mydb_trace_key(key->data, key->size, b1), by_successor ? "successor" : "predecessor",
              mydb_trace_key(nkey.data, nkey.size, b2), (unsigned)holder);
    }
    eDBState result = FAIL;
    if (DONE == block_change(x, &nkey, &nval, k, MYDB_INVALIDPTR) &&
        DONE == block_delete(h, &nkey, by_successor))
    {
        block_write(x);
        block_write(h);
        result = DONE;
    }
    free(buf);
    //-----------------------------------------
    if (result == DONE)
        release_empty_subtree(db, side);
    return result;
}
//-------------------------------------------------------------------------------------------------------------
/*  block is the non-full root. Returns 0 - deleted, -1 - not found or error,
 *  1 - a block was split on the way: the caller must make the root non-full and call again
 *  (so every block on the path has a room for the key replacement).
 */
int block_delete_deep(IN sBlock *block, IN const sDBT *key)
{
    sDB *db = block->owner_db_;
    uint32_t xoff = block->offset_;
    bool split = false;
    //-----------------------------------------
    for (;;)
    {
        sBlock *x = block_create(db, xoff);
        if (!x)
            return FAIL;

        sDBT iter, *k = block_find_ge(x, key, &iter);
        bool hit = (k && !key_compare(k, key));
        //-----------------------------------------
        if (*block_type(x) == Leaf)
        {
            TRACE(MYDB_TRACE_TREE, "page %u leaf, %u keys: %s", (unsigned)xoff, (unsigned)*block_nkvs(x),
                  hit ? "remove the key" : "not found");
            if (!hit || DONE != block_delete(x, key, false))
                return FAIL; /* not found */
            block_write(x);
            return 0;
        }
        //-----------------------------------------
        uint32_t yoff = block_lptr(x, k);
        if (!hit)
        {
            TRACE(MYDB_TRACE_TREE, "page %u inner, %u keys -> child page %u", (unsigned)xoff,
                  (unsigned)*block_nkvs(x), (unsigned)yoff);
            if (DONE != split_if_full(db, xoff, yoff, &split))
                return FAIL;
            if (split)
            {
                TRACE(MYDB_TRACE_TREE, "a page was split on the way: start again from the root");
                return 1;
            }
            xoff = yoff;
            continue;
        }
        //-----------------------------------------
        /* The key is in the inner block x: the predecessor */
        TRACE(MYDB_TRACE_TREE, "page %u inner: the key is here, look for a neighbour key", (unsigned)xoff);
        uint32_t holder;
        int rc = prepare_spine(db, xoff, yoff, true, &holder);
        if (rc < 0)
            return FAIL;
        if (rc > 0)
            return 1;
        if (holder != MYDB_INVALIDPTR)
            return (DONE == replace_by_neighbour(db, xoff, key, holder, false)) ? 0 : -1;
        //-----------------------------------------
        /* the left subtree is empty: the successor */
        if (!(x = block_create(db, xoff)))
            return FAIL;
        k = block_find_ge(x, key, &iter);
        uint32_t zoff = block_rptr(x, k);

        rc = prepare_spine(db, xoff, zoff, false, &holder);
        if (rc < 0)
            return FAIL;
        if (rc > 0)
            return 1;
        if (holder != MYDB_INVALIDPTR)
            return (DONE == replace_by_neighbour(db, xoff, key, holder, true)) ? 0 : -1;
        //-----------------------------------------
        /* both subtrees are empty: remove the key with its right subtree */
        TRACE(MYDB_TRACE_TREE, "page %u: both subtrees of the key are empty, remove it with the right one",
              (unsigned)xoff);
        if (!(x = block_create(db, xoff)))
            return FAIL;
        k = block_find_ge(x, key, &iter);
        zoff = block_rptr(x, k);
        if (DONE != block_delete(x, key, false))
            return FAIL;
        block_write(x);
        release_empty_subtree(db, zoff);
        return 0;
    }
}
//-------------------------------------------------------------------------------------------------------------
