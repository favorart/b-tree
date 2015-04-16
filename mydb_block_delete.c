#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"

//-------------------------------------------------------------------------------------------------------------
typedef struct childs_list sChList;
struct childs_list
{
  struct Block       *block;
  struct childs_list *next;
};
void  childs_list_free (sChList *head)
{

  for ( sChList *l_child = head; l_child; l_child = head )
  {
    head = l_child->next;

    block_destroy (l_child->block);
    free (l_child);
  }
}
//-------------------------------------------------------------------------------------------------------------
bool  block_recursively_delete_key_in_left_branch (IN sBlock *parent, IN sBlock *ychild, IN const sDBT *key)
{
  const char *error_prefix = "memory block left branch";
  bool  fail = false;
  //-----------------------------------------
  sBlock *cur_child;

  sChList  *l_child = NULL, **l_head = &l_child;
  //-----------------------------------------
  /*  находим k - предшественника key в поддереве,
  *  корнем которого является y. Рекурсивно удаляем
  *  k и заменяем key в x ключом k.
  *  (поиск k за один проход вниз)
  *
  *   x: ..key..      x: ..(k->key)..
  *       /   \   =>          /   \
  *      y     z             y->k  z
  */
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (ychild, &iter, NULL);
  /* Find last right key in left Leaf, that less the given key */
  while ( k && key_compare (k, key) > 0 )
  {
    k = block_key_next (ychild, k, NULL);
  }

  cur_child = ychild;
  /* instead of recursion using loop */
  while ( l_child && (*block_type (l_child->block)) != Leaf )
  {
    if ( l_child )
      l_child = l_child->next;
    //-----------------------------------------
    if ( !(l_child = calloc (1U, sizeof (*l_child))) )
    {
      fprintf (stderr, "%s%s", error_prefix, strerror (errno));
      fail = true;
      goto LBRANCH_FREE;
    }
    //-----------------------------------------
    l_child->block = block_create (cur_child->owner_db_, *block_rptr (cur_child, k));
    //-----------------------------------------
    cur_child = l_child->block;
    /* move to the last k (key) in current block, ohhh */
    iter.size = 0U;
    k = block_key_next (cur_child, &iter, NULL);
    uint_t count = *block_nkvs (cur_child);
    while ( count-- )
    {
      k = block_key_next (cur_child, &iter, NULL);
    }
    //-----------------------------------------
  } // end while recursion

    // {
    //   sDBT *k_value = { 0 };
    //   // block_key_data (cur_child, k, &k_value); <-- malloc'ed
    //   uint_t vsz = ((uint_t*) k->data - 1U);
    // 
    //   block_delete (cur_child, k);
    // }


    // block_delete (block, key);
    // block_insert (block, k, value);
    // x[key] = x[k]
    //-----------------------------------------
LBRANCH_FREE:;
  childs_list_free (*l_head);
  //-----------------------------------------
  return fail;
}
bool  block_recursively_delete_key_in_rght_branch (IN sBlock *parent, IN sBlock *zchild, IN const sDBT *key)
{
  const char *error_prefix = "memory block right branch";
  bool  fail = false;
  //-----------------------------------------
  sBlock *cur_child = NULL;

  sChList  *l_child = NULL, **l_head = &l_child;
  //-----------------------------------------
  sDBT iter = { 0 };
  sDBT   *k = block_key_next (zchild, &iter, NULL);
  while ( k && key_compare (k, key) < 0 )
  {
    k = block_key_next (zchild, k, NULL);
  }
  //-----------------------------------------
  if ( (*block_type (zchild)) == Leaf )
  {
    sDBT val = { 0 };
    val.size = block_key_data (zchild, k, &val.data);
    //-----------------------------------------------
    block_delete (zchild, k);
    // ?!?!?! Get k value - ThatS RECURSION 
  }
  else
  {
    block_recursively_delete_key_in_rght_branch (parent, zchild, key);
  }
  // block_delete (parent, key);
  // block_insert (parent, k, value);
  // x[key] = x[k]
  //-----------------------------------------
RBRANCH_FREE:;
  childs_list_free (*l_head);
  //-----------------------------------------
  return fail;
}
//-------------------------------------------------------------------------------------------------------------
