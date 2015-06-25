#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"

//-------------------------------------------------------------------------------------------------------------
typedef struct childs_list sChList;
struct childs_list
{
  struct Block  *parent;
  struct Block  *lchild;
  struct Block  *rchild;
  //------------------------
  struct childs_list *next;
};

void  childs_list_free (sChList *head)
{
  for ( sChList *ch_list = head; ch_list; ch_list = head )
  {
    head = ch_list->next;
    //----------------------------------
    if ( ch_list->next )
    {
      if ( ch_list->next->parent == ch_list->lchild )
        ch_list->lchild = NULL;
      else
        ch_list->rchild = NULL;
    }
    //----------------------------------
    block_destroy (ch_list->parent);
    block_destroy (ch_list->lchild);
    block_destroy (ch_list->rchild);
    //----------------------------------
    free (ch_list);
  }
}

// рекурсивный спуск
bool  block_recursive_descent (IN sBlock *parent, IN sBlock *lchild, IN sBlock *rchild, IN const sDBT *key, bool is_right);

eDBState block_deep_del (IN sBlock *block, IN const sDBT *key)
{
  const char *error_prefix = "memory block deep delete";
  eDBState result = DONE;
  /*-----------------------------------------
  *   while ( *block_type (parent) != Leaf )
  *   {
  *     if ( block_select_data (parent, key) && block_enough (rchild) )
  *     {
  *       block_recursive_descent (parent, lchild, rchild); // EXCESS
  *       parent = rchild;
  *     }
  *     else if ( block_enough (lchild) ) // EXCESS
  *     {
  *       block_recursive_descent (parent, lchild, rchild);
  *       parent = lchild;
  *     }
  *     else
  *     {
  *       block_merge (parent, lchild, rchild); // ROTATE RIGHT if choosen first case
  *       parent = lchild;
  *     }
  *   }
  *-----------------------------------------*/

  //-----------------------------------------
  sChList  *rec_list = NULL, *rec_list_head = NULL;
  //-----------------------------------------
  rec_list_head = calloc (1U, sizeof (*rec_list_head));
  if ( !rec_list_head )
  {
    fprintf (stderr, "%s%s", error_prefix, strerror (errno));
    result = FAIL;
    goto DDEL_FREE;
  }
  //-----------------------------------------
  rec_list_head->parent = block;
  
  sBlock *match = NULL;
  sDBT *kkey = NULL;
  bool is_right = true;
  
  bool k_left;
  bool k_rght;
  //-----------------------------------------
  rec_list = rec_list_head;
  while ( *block_type (rec_list->parent) != Leaf )
  {
    //-----------------------------------------
    if ( !match )
    {
      sDBT iter = { 0 }, *prev_k = NULL;
      sDBT   *k = block_key_next (rec_list->parent, &iter, NULL);
      while ( k && key_compare (k, key) < 0 )
      {
        prev_k = k;
        k = block_key_next (rec_list->parent, k, NULL);
      }

      if ( !k )
      {
        is_right = true;
        kkey = prev_k;
      }
      else
      {
        is_right = false;
        kkey = k;
      }
      if ( !kkey )
      {
        fprintf (stderr, "%s%s", error_prefix, strmyerror (MYDB_ERR_FPARAM));
        result = FAIL;
        goto DDEL_FREE;
      }
    }
    //-----------------------------------------
    rec_list->lchild = block_create (rec_list->parent->owner_db_, *block_lptr (rec_list->parent, kkey));
    rec_list->rchild = block_create (rec_list->parent->owner_db_, *block_rptr (rec_list->parent, kkey));
    //-----------------------------------------
    rec_list->next = calloc (1U, sizeof (*rec_list));
    if ( !rec_list->next )
    {
      fprintf (stderr, "%s%s", error_prefix, strerror (errno));
      result = FAIL;
      goto DDEL_FREE;
    }    
    //-----------------------------------------
    if ( !key_compare (kkey, key) && block_enough (rec_list->rchild) )
    {
      /* Question is only when (k == key), else there are not questions */
      // is_right = false;

      match = rec_list->parent;
      // block_recursive_descent // EXCESS
      rec_list->parent = rec_list->rchild;
    }
    else
    {
      block_recursive_descent (rec_list->parent,
                               rec_list->lchild,
                               rec_list->rchild,
                               key,    is_right);
      rec_list->parent = ( is_right ) ? rec_list->rchild : rec_list->lchild;
    }
    //-----------------------------------------
    rec_list = rec_list->next;
    //-----------------------------------------
  }
  //-----------------------------------------
  if ( !match )
  {
    sDBT iter = { 0 };
    sDBT   *k = block_key_next (rec_list->parent, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    { k = block_key_next (rec_list->parent, k, NULL); }

    if ( !key_compare (k, key) )
    {
      // block_delete (rec_list->parent, key);
    }
    else
    {
      result = FAIL;
    }
  }
  else // match!
  {
    sDBT iter = { 0 }, *prev_k = NULL;
    sDBT   *k = block_key_next (rec_list->parent, &iter, NULL);
    while ( k && key_compare (k, key) < 0 )
    {
      prev_k = k;
      k = block_key_next (rec_list->parent, k, NULL);
    }
    kkey = k ? k : prev_k;

    sDBT val = block_key_data (block, kkey);
    // block_change (match, key, &val, kkey);
    // block_delete (rec_list->parent, kkey);
  }
  //-----------------------------------------
DDEL_FREE:;
  rec_list_head->parent = NULL;
  childs_list_free (rec_list_head);
  rec_list_head = NULL;
  //-----------------------------------------
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
bool  block_recursive_descent (IN sBlock *parent, IN sBlock *lchild, IN sBlock *rchild, IN const sDBT *key, bool is_right)
{
  const char *error_prefix = "memory block recursive descent";
  bool  is_fail = false;
  //-----------------------------------------
  // rotate left
  if ( !block_enough (lchild) && block_enough (rchild) )
  {
    /*-----------------------------------------
     *  находим k - предшественника key в поддереве,
     *  корнем которого является lchild. Рекурсивно
     *  удаляем k и заменяем key в parent ключом k.
     *  (поиск k за один проход вниз)
     *
     *  par: ..key..      par: ..(key<-k)..
     *        /   \    =>       /        \
     *     lch     rch       lch<-key  k<-rch
     *------------------------------------------*/
    sDBT  iter = {0};
    sDBT  *k = block_key_next (parent, &iter, NULL);
    sDBT val = block_key_data (parent, k);

    block_insert (lchild, k, &val, NULL, MYDB_INVALIDPTR);
    // block_delete (parent, k); // MOVING!!!

      k = block_key_next (rchild, &iter, NULL);
    val = block_key_data (rchild, k);

    block_insert (parent, k, &val, NULL, MYDB_INVALIDPTR);
    // block_delete (rchild, k);
  }
  //-----------------------------------------
  // rotate right
  if ( block_enough (lchild) && !block_enough (rchild) )
  {
    /*-----------------------------------------
     *   находим k - следующий за key ключ в поддереве,
     *   корнем которого является rchild. Рекурсивно
     *   удаляем k и заменяем key в parent ключом k.
     *   (поиск k за один проход вниз)
     *
     *   (симметрично обращаемся к дочернему по отношению
     *    к parent узлу rchild, который слудует за ключом
     *    key в узле parent)
     *
     *  par: ..key..      par: ..(k->key)..
     *        /   \    =>       /        \
     *     lch     rch       lch->k  key->rch
     *-----------------------------------------*/

  }
  //-----------------------------------------
  // merge to left
  else if ( !block_enough (lchild) && !block_enough (rchild) )
  {
    /*-----------------------------------------
     *  par: ..k..        par: .....
     *        / \      =>       /
     *     lch   rch           (lch)k(rch)
     *-----------------------------------------*/
    block_merge_child (parent, lchild, rchild, key);
  }
  //----------------------------
  return is_fail;
}
//-------------------------------------------------------------------------------------------------------------
bool  block_recursively_delete_key_in_left_branch (IN sBlock *parent, IN sBlock *ychild, IN const sDBT *key)
{
  const char *error_prefix = "memory block left branch";
  bool  fail = false;
  //-----------------------------------------
//  sBlock *cur_child;
//
//  sChList  *l_child = NULL, **l_head = &l_child;
// 
//  sDBT iter = { 0 };
//  sDBT   *k = block_key_next (ychild, &iter, NULL);
//  /* Find last right key in left Leaf, that less the given key */
//  while ( k && key_compare (k, key) > 0 )
//  {
//    k = block_key_next (ychild, k, NULL);
//  }
//
//  cur_child = ychild;
//  /* instead of recursion using loop */
//  while ( l_child && (*block_type (l_child->block)) != Leaf )
//  {
//    if ( l_child )
//      l_child = l_child->next;
//    //-----------------------------------------
//    if ( !(l_child = calloc (1U, sizeof (*l_child))) )
//    {
//      fprintf (stderr, "%s%s", error_prefix, strerror (errno));
//      fail = true;
//      goto LBRANCH_FREE;
//    }
//    //-----------------------------------------
//    l_child->block = block_create (cur_child->owner_db_, *block_rptr (cur_child, k));
//    //-----------------------------------------
//    cur_child = l_child->block;
//    /* move to the last k (key) in current block, ohhh */
//    iter.size = 0U;
//    k = block_key_next (cur_child, &iter, NULL);
//    uint_t count = *block_nkvs (cur_child);
//    while ( count-- )
//    {
//      k = block_key_next (cur_child, &iter, NULL);
//    }
//    //-----------------------------------------
//  } // end while recursion
//
//    // {
//    //   sDBT *k_value = { 0 };
//    //   // block_key_data (cur_child, k, &k_value); <-- malloc'ed
//    //   uint_t vsz = ((uint_t*) k->data - 1U);
//    // 
//    //   block_delete (cur_child, k);
//    // }
//
//
//    // block_delete (block, key);
//    // block_insert (block, k, value);
//    // x[key] = x[k]
    //-----------------------------------------
LBRANCH_FREE:;
//  childs_list_free (*l_head);
  //-----------------------------------------
  return fail;
}
bool  block_recursively_delete_key_in_rght_branch (IN sBlock *parent, IN sBlock *zchild, IN const sDBT *key)
{
  const char *error_prefix = "memory block right branch";
  bool  fail = false;
  //-----------------------------------------
//  sBlock *cur_child = NULL;
//
//  sChList  *l_child = NULL, **l_head = &l_child;
//  //-----------------------------------------
//  sDBT iter = { 0 };
//  sDBT   *k = block_key_next (zchild, &iter, NULL);
//  while ( k && key_compare (k, key) < 0 )
//  {
//    k = block_key_next (zchild, k, NULL);
//  }
//  //-----------------------------------------
//  if ( (*block_type (zchild)) == Leaf )
//  {
//    sDBT val = { 0 };
//    val.size = block_key_data (zchild, k, &val.data);
//    //-----------------------------------------------
//    block_delete (zchild, k);
//    // ?!?!?! Get k value - ThatS RECURSION 
//  }
//  else
//  {
//    block_recursively_delete_key_in_rght_branch (parent, zchild, key);
//  }
//  // block_delete (parent, key);
//  // block_insert (parent, k, value);
//  // x[key] = x[k]
  //-----------------------------------------
RBRANCH_FREE:;
//  childs_list_free (*l_head);
  //-----------------------------------------
  return fail;
}
//-------------------------------------------------------------------------------------------------------------
eDBState  block_rotate_left (IN sBlock *block, IN sDBT *key)
{

  // get last

  // sDBT  iter = { 0 }, val = { 0 };
  // sDBT  *k = block_key_next (parent, &iter, NULL);
  // val.size = block_key_data (parent, &iter, &val);

  sDBT iter = { 0 }, *k_prev = NULL;
  sDBT   *k = block_key_next (block, &iter, NULL);
  while ( k && key_compare (key, k) < 0 )
  {
    k = block_key_next (block, k, NULL);
    k_prev = k;
  }

  sDBT val = block_key_data (block, k_prev);



  return DONE;
}
eDBState  block_rotate_rght (IN sBlock *block, IN sBlock *lchild, IN sBlock *rchild, IN const sDBT *key)
{
  sDBT cur_k = { 0 }, value = { 0 };

  // get first
  if ( DONE != block_select (block, key, &cur_k, &value) )
    return FAIL;

  // sBlock *lchild = block_create (block, block_lptr (block, &cur_k));
  // sBlock *rchild = block_create (block, block_rptr (block, &cur_k));

  // block_change ();


  return DONE;
}
//-------------------------------------------------------------------------------------------------------------

