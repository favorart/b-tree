#include "stdafx.h"
#include "mydb_block_low.h"
#include "mydb_block.h"
#include "mydb_techb.h"

//-------------------------------------------------------------------------------------------------------------
void      compose (OUT uint32_t *offset, IN  IN  uint32_t sz_page,
                   IN  uint_t  ipage, IN  uint_t  ibyte, IN  uint_t  ibit)
{
  *offset = ibit + (ibyte + ipage * sz_page) * MYDB_BITSINBYTE;
}
void    decompose (IN  uint32_t offset, IN  uint32_t sz_page,
                   OUT uint_t *ipage, OUT uint_t *ibyte, OUT uint_t *ibit)
{
  /* offset = ibit + (ibyte + ipage * sz_page) * BITSINBYTE */

  *ibit  = (offset % MYDB_BITSINBYTE);
  *ibyte = (offset / MYDB_BITSINBYTE % sz_page);
  *ipage = (offset / MYDB_BITSINBYTE / sz_page);
}
//-------------------------------------------------------------------------------------------------------------
/* set a bit with given offset in tech.blocks array of db */
eTBState   techb_set_bit (IN sDB *db, IN uint32_t offset, IN  bool  bit)
{
  uint_t  ipage, ibyte, ibit, sz_techb = (db->head_.page_size_);
  uchar_t  *byte;

  if ( offset >= sz_techb )
    return FAIL;
  //-----------------------------------------
  decompose (offset, sz_techb, &ipage, &ibyte, &ibit);
  byte  = (&db->techb_arr_[ipage].memory_[ibyte]);

   if ( bit ) *byte |=  (1U << (MYDB_BITSINBYTE - ibit - 1U));
   else       *byte &= ~(1U << (MYDB_BITSINBYTE - ibit - 1U));
  //-----------------------------------------
  return DONE;
}
/* returns the value of a bit by given offset */
eTBState   techb_get_bit (IN sDB *db, IN uint32_t offset, OUT bool *bit)
{
  uint_t  ipage, ibyte, ibit, sz_techb = db->head_.page_size_;
  uchar_t *byte;

  if ( offset >= sz_techb )
    return FAIL;
  //-----------------------------------------
  decompose (offset, sz_techb, &ipage, &ibyte, &ibit);

   byte = &db->techb_arr_[ipage].memory_[ibyte];
  *bit = ((*byte & (1U << (MYDB_BITSINBYTE - ibit - 1U))) != 0U);
  //-----------------------------------------
  return DONE;
}
/* returns the offset of first free bit in tech.blocks array of db */
uint32_t   techb_get_index_of_first_free_bit (IN sDB *db)
{
  uint32_t  offset = db->techb_last_free;
  //-----------------------------------------
  uint_t   c_techb = db->head_.techb_count_;
  uint_t  sz_techb = db->head_.page_size_;
  //-----------------------------------------
  uint_t in_page, in_byte, in_bit;
  decompose (offset, sz_techb, &in_page, &in_byte, &in_bit);
  //-----------------------------------------
  for ( uint_t ipage = in_page; ipage < c_techb; ++ipage )
    for ( uint_t ibyte = in_byte; ibyte < sz_techb; ++ibyte )
      for ( uint_t  ibit = in_bit; ibit < MYDB_BITSINBYTE; ++ibit )
      { 
        uchar_t  *byte = &db->techb_arr_[ipage].memory_[ibyte];
        if ( !(*byte & (1U << (MYDB_BITSINBYTE - ibit - 1U))) )
        { 
          *byte |= (1U << (MYDB_BITSINBYTE - ibit - 1U));
          compose (&db->techb_last_free, sz_techb, ipage, ibyte, ibit);
          return db->techb_last_free;          
        }
      }
  //-----------------------------------------
  return db->head_.block_count_;
}
//-------------------------------------------------------------------------------------------------------------
/* drop the changes in the db tech.blocks to the disk */
eDBState   techb_sync    (IN sDB *db)
{
  const char *error_prefix = "technical block syncronization";
  if ( !db->techb_arr_ )
  {
    fprintf (stderr, "%s%s Whole array.\n", error_prefix,
             strmyerror (MYDB_ERR_FPARAM));
    return FAIL;
  }
  //-----------------------------------------
  for ( uint_t i = 0U; i < db->head_.techb_count_; ++i )
    if( db->techb_arr_[i].dirty_ )
      if ( DONE != block_write (&db->techb_arr_[i], false) )
      {
        mydb_errno = MYDB_ERR_OFFSET;
        fprintf (stderr, "%s%s Offset is %d.\n", error_prefix,
                 strmyerror (mydb_errno), db->techb_arr_[i].offset_);
        // return FAIL;
      }
  //-----------------------------------------
  return DONE;
}
//-------------------------------------------------------------------------------------------------------------
/* clear the data in techb, but do not free itself */
void       techb_destroy (IN sTechB  *techb)
{
 free   (techb->memory_);
 memset (techb, 0, sizeof (*techb));
}
/* ! assign the given memory */
sTechB*    techb_create  (IN sDB     *db,
                          IN uchar_t *memory,
                          IN uint_t   offset)
{
  const char *error_prefix = "technical block creation";
  bool     fail  = false;
  sTechB  *techb = NULL;
  //-----------------------------------------
  if ( offset >= db->head_.techb_count_ )
  {
    fail = true;
    mydb_errno = MYDB_ERR_OFFSET;
    fprintf (stderr, "%s%s\n", error_prefix, strmyerror (mydb_errno));
    goto TECHB_FREE;
  }
  //-----------------------------------------
  if ( (techb = (sTechB*) memory) )
  {
    memset (techb, 0, sizeof (*techb));

    techb->owner_db_ = db;
    techb->size_   = db->head_.page_size_;
    techb->offset_ = offset;
    techb->dirty_  = false;
    
    techb->memory_ = calloc (techb->size_, sizeof (uchar_t));
    if ( !techb->memory_ )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto TECHB_FREE;
    }
    //-----------------------------------------
    if ( DONE != block_seek (techb, false) )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto TECHB_FREE;
    }
   
    if ( read (db->hfile_, techb->memory_, techb->size_) != techb->size_ )
    {
      fail = true;
      fprintf (stderr, "%s%s\n", error_prefix, strerror (errno));
      goto TECHB_FREE;
    }
  } // end if
  else fprintf (stderr, "%s%s\n", error_prefix, strmyerror (MYDB_ERR_FPARAM));
  //-----------------------------------------
TECHB_FREE:;
  if ( fail )
  {
    techb_destroy (techb);
    techb = NULL;
  }
  //-----------------------------------------
  return techb;
}
//-------------------------------------------------------------------------------------------------------------
