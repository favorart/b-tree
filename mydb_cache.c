#include "stdafx.h"
#include "mydb_block.h"
#include "mydb_cache.h"
 

// #define _DEBUG_CACHE
/* Перед изменением блока, запись попадает в журнал */
//-------------------------------------------------------------------------------------------------------------
struct Cache_CyclicHashTable_BuldInList
{
  sBlock    *block; // ptr to a cell in the blocks array
  uint32_t  offset; // = 0 for an empty cell
  //----------------
  sCchHL     *next;
  sCchHL     *prev;
  //----------------
};
//-------------------------------------------------------------------------------------------------------------
#define CACHE_HASH_COEF 1.5

static hash_t  cache_hash (IN uint32_t offset, IN hash_t size)
{
  hash_t hash = 0U;
  unsigned char* pc = (unsigned char*) &offset;

  for ( int i = 0; i < sizeof (offset); ++i, ++pc )
  {
    hash += *pc;
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);

  return (hash % size);
}
static hash_t  cache_hash_get (IN sDB *db, IN uint32_t offset)
{
  sCache *Cch = db->cache_;
  //-----------------------------------------------
  /* 0 - пустой элемент */
  ++offset;
  
  hash_t h = cache_hash (offset, Cch->szHash_), he = h;
  while ( /* (h < Cch->szHash_) && */ Cch->blHash_[h].offset )
  { 
    /* идём по открытой адресации до нахождения или 0 */
    if ( Cch->blHash_[h].offset == offset )
      return h;

    // ++h;
    h = (h + 1U) % Cch->szHash_;

    // if ( he == h ) break; // !!!
  }
  //-----------------------------------------------
  return Cch->szHash_;
}
static hash_t  cache_hash_set (IN sDB *db, IN uint32_t offset)
{
  sCache *Cch = db->cache_;
  //-----------------------------------------------
  /* 0 - пустой элемент */
  ++offset;

  hash_t h = cache_hash (offset, Cch->szHash_), he = h;
  while ( /* (h < Cch->szHash_) && */ Cch->blHash_[h].offset )
  {
    /* идём по открытой адресации до нахождения нуля */
    hash_t hn = cache_hash (Cch->blHash_[h].offset, Cch->szHash_);
    /* проверяем, что все элементы в открытой адрерсации,
     * большие по хэшу данного находится ЗА ним.
     */
    if ( hn > he )
    {
      /* если это не так, делаем 'ЗА' */
      // he = h++;
      he = h; 
      h = (h + 1U) % Cch->szHash_;
      /* место куда нужно вставить */

      /* считаем все последующие */
      while ( /* (h < Cch->szHash_) && */ Cch->blHash_[h].offset )
      { 
        // ++h;
        h = (h + 1U) % Cch->szHash_;
      }
      
      // if ( h == Cch->szHash_ )
      //   return h; // !!! ERROR INSERTING

      /* раздвигаем в этом месте */
      while ( h != he )
      {
        /* сдвигаем вверх все последующие */
        SWAP (&Cch->blHash_[h - 1U], &Cch->blHash_[h]);

        //---LRU List---------------------------------
        if ( Cch->blHash_[h].next )
          Cch->blHash_[h].next->prev = &Cch->blHash_[h];
        else
          Cch->LRULast = &Cch->blHash_[h];
        
        if ( Cch->blHash_[h].prev )
          Cch->blHash_[h].prev->next = &Cch->blHash_[h];
        else
          Cch->LRUHead = &Cch->blHash_[h];
        //--------------------------------------------

        // --h;
        h = (h) ? (h - 1U) : (Cch->szHash_ - 1U);
      }
      /* записываем исходный блок на освободившееся место */
      Cch->blHash_[he].offset = offset;
      return he;
    }
    // ++h;
    h = (h + 1U) % Cch->szHash_;
  } // end while

  // if ( h == Cch->szHash_ )
  // {
  //   return h; // !!! ERROR INSERTING
  // }
  /* Записываем в первый чистый элемент */
  // else if ( !Cch->blHash_[h].offset )
  { Cch->blHash_[h].offset = offset; }
  //-----------------------------------------------
  return h;
}
static hash_t  cashe_hash_pop (IN sDB *db, IN hash_t hash)
{
  sCache *Cch = db->cache_;
  //-----------------------------------------------
  /* элемент, который нужно удалить */
  memset (&Cch->blHash_[hash], 0, sizeof (*Cch->blHash_));

  // ++hash;
  hash = (hash + 1U) % Cch->szHash_;
  /* проверим следующий */
  while ( /* (hash < Cch->szHash_) && */ Cch->blHash_[hash].offset )
  {
    /* проверяем, если он находится в открытой адресации */
    hash_t hn = cache_hash (Cch->blHash_[hash].offset, Cch->szHash_);
    if ( hash > hn )
    {
      // hash_t h = hash - 1U;
      hash_t h = (hash) ? (hash - 1U) : (Cch->szHash_ - 1U);
      /* сдвигаем вверх */
      Cch->blHash_[h] = Cch->blHash_[hash];

      /*---LRU List---------------------------------*/
      if ( Cch->blHash_[h].next )
        Cch->blHash_[h].next->prev = &Cch->blHash_[h];
      else
        Cch->LRULast = &Cch->blHash_[h];

      if ( Cch->blHash_[h].prev )
        Cch->blHash_[h].prev->next = &Cch->blHash_[h];
      else
        Cch->LRUHead = &Cch->blHash_[h];
      /*--------------------------------------------*/

      /* элемент, который нужно удалить */
      memset (&Cch->blHash_[hash], 0, sizeof (*Cch->blHash_));
    }
    else break;
    /* проверяем, следующий */
    // ++hash;
    hash = (hash + 1U) % Cch->szHash_;
  }
  //-----------------------------------------------
  return (hash) ? (hash - 1U) : (Cch->szHash_ - 1U); // (hash - 1U);
}
//-------------------------------------------------------------------------------------------------------------
bool  mydb_cache_init (IN sDB *db)
{
  bool was_fail = false;
  //-----------------------------------------------
  db->cache_ = (sCache*) calloc (1U, sizeof (*db->cache_));
  if ( db->cache_ )
  {
    sCache *Cch = db->cache_;
    //----------------------------------------------
    memset (Cch, 0, sizeof (*Cch));
    
    Cch->m_size_ = db->head_.cache_size_;
    Cch->  n_pgs = 0;
    Cch->all_pgs = db->head_.cache_size_ / db->head_.page_size_;
    Cch-> sz_pg  = db->head_.page_size_;
    Cch->szHash_ = (uint_t) ((double) Cch->all_pgs * CACHE_HASH_COEF);
    //-----------------------------------------------
    Cch->blocks_ = (sBlock*)   calloc (Cch->all_pgs,  sizeof (*Cch->blocks_));
    Cch->blHash_ = (sCchHL*)   calloc (Cch->szHash_,  sizeof (*Cch->blHash_));
    Cch->memory_ = (uchar_t*)  malloc (Cch->all_pgs * Cch->sz_pg * sizeof (uchar_t));

    if ( !Cch->blocks_ || !Cch->memory_ || !Cch->blHash_ )
    {
      fprintf (stderr, "chache init%s", strerror (errno));
      was_fail = true;
      mydb_cache_free (db);
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
    fprintf (stderr, "chache init%s", strerror (errno));
    was_fail = true;
  }
  //-----------------------------------------------
  return was_fail;
}
bool  mydb_cache_push (IN sDB *db, IN uint32_t offset, OUT sBlock **block)
{
  sCache   *Cch = db->cache_;
  bool  in_hash = false;

  sCchHL *hl = NULL;
  //-----------------------------------------------
  hash_t h = cache_hash_get (db, offset);
  if ( h >= Cch->szHash_ )
  {
    sBlock  *hl_block  = NULL;
    uchar_t *hl_memory = NULL;
    //-----------------------------------------------
    /* если в кэше нет данного блока */
    if ( Cch->n_pgs == Cch->all_pgs )
    {
      //-----------------------------------------------
      /*  Кэш заполнен.
       *  Грязный блок должен быть записан
       *  на диск перед вытеснением из кэша.
       */
      block_dump (Cch->LRULast->block); // check error?
      // block_free (Cch->LRULast->block);
      //-----------------------------------------------
      /* сохраняем указатель на освободившийся блок */
      hl_block = Cch->LRULast->block;
      
      /* вытесняем из кэша */
      h = (Cch->LRULast - Cch->blHash_);
      //---LRU List------------------------------------
      Cch->LRULast = Cch->LRULast->prev;
      Cch->LRULast->next = NULL;
      //-----------------------------------------------
      cashe_hash_pop (db, h);
      //-----------------------------------------------
    }
    //-----------------------------------------------
    /* кэш не заполнен - добавляем */
    h = cache_hash_set (db, offset);  // can be error
    if ( h == Cch->szHash_ )
    { fprintf (stderr, "cache set%s\n", " error"); // strmyerror (MYDB_ERR_CCHSET));
      exit (EXIT_FAILURE); // !!!
    }
    //-----------------------------------------------
    hl = &Cch->blHash_[h];
    if ( Cch->n_pgs == Cch->all_pgs )
    {
      hl->block = hl_block;
      hl_memory = hl_block->memory_;
    }
    else
    {
      hl->block = &Cch->blocks_[Cch->n_pgs];
      hl_memory = (Cch->memory_ + Cch->n_pgs * Cch->sz_pg);
      ++Cch->n_pgs;
    }
    //-----------------------------------------------
    // block_free () <--
    memset (hl->block, 0, sizeof (*hl->block));
    memset (hl_memory, 0, sizeof (Cch->sz_pg));
    hl->block->memory_ = hl_memory;

    //---LRU List------------------------------------
    hl->prev = NULL;
    if ( Cch->LRUHead->offset )
    {
      /* если самая первая вставка в кэш */
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
    /* если в кэше данный блок есть */
    hl = &Cch->blHash_[h];
    in_hash = true;

#ifdef _DEBUG_CACHE
    printf ("exact\n");
#endif // _DEBUG
  }
  //-----------------------------------------------
  (*block) = hl->block;
  (*block)->offset_ = offset;

  //-----------------------------------------------
#ifdef _DEBUG_CACHE
  for ( uint_t i = 0U; i < Cch->szHash_; ++i )
    printf ("%2d offset=%2d prev=%2d next=%2d\n", i, Cch->blHash_[i].offset,
    Cch->blHash_[i].prev ? (Cch->blHash_[i].prev - Cch->blHash_) : -1,
    Cch->blHash_[i].next ? (Cch->blHash_[i].next - Cch->blHash_) : -1);
  printf ("last index: %2d\n", Cch->LRULast - Cch->blHash_);
  printf ("head index: %2d\n", Cch->LRUHead - Cch->blHash_);
  printf ("nmbr pages: %2d\n\n", Cch->n_pgs);
#endif // _DEBUG
  //-----------------------------------------------
  return in_hash;
}
void  mydb_cache_sync (IN sDB *db)
{
  sCache *Cch = db->cache_;
  //-----------------------------------------------
  for ( sCchHL *hl = Cch->LRUHead;
                hl != Cch->LRULast;
                hl  = hl->next )
  {
    block_dump (hl->block); // check error?
    // block_free (hl->block);
    // hl->block = NULL;
  }
  //-----------------------------------------------
  // block_free ()
  memset (Cch->blocks_, 0, Cch->all_pgs * sizeof (*Cch->blocks_));
  memset (Cch->blHash_, 0, Cch->all_pgs * sizeof (*Cch->blHash_));
  memset (Cch->memory_, 0, Cch->all_pgs * Cch->sz_pg);
  //-----------------------------------------------
  Cch->LRUHead = Cch->blHash_;
  Cch->LRULast = Cch->blHash_;
  Cch->n_pgs = 0;
  //-----------------------------------------------
}
void  mydb_cache_free (IN sDB *db)
{
  if ( db->cache_ )
  {
    free (db->cache_->blocks_);
    free (db->cache_->memory_);
    free (db->cache_->blHash_);
  }
  //-----------------------------------------------
  free (db->cache_);
  db->cache_ = NULL;
}
//-------------------------------------------------------------------------------------------------------------
