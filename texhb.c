#include "stdafx.h"
#include "block.h"
#include "techb.h"

//-------------------------------------------------------------------------------------------------------------
/* offset must lie in the interval [0, tch_blks_count-1] */
/* returns DONE/FAIL */
eTBState   techb_set_bit (IN sDB *db, IN uint32_t offset, IN bool bit)
{
 uint_t  byte  = offset / (BITSINBYTE);
 uint_t  ipage = offset / (BITSINBYTE * db->head_.page_size_);
 //-----------------------------------------
 // db->techb_arr_[ipage].memory_

 byte &= (1U << (offset % BITSINBYTE));
 //-----------------------------------------
 return byte ? NODE : FREE;
}
/* techb_get_bit returns the offset of first free bit, if flag is active. */
/* Otherwise it returns the value of a bit by given offset. */
uint32_t   techb_get_bit (IN sDB *db, IN uint32_t offset, IN bool first_free)
{
  // uint32_t  offset = 0;
  
  // long  file_position = tell (db->hfile_);
  // //-----------------------------------------
  // lseek (db->hfile_, sizeof (sDBFH), SEEK_SET);
  // 
  // for ( uint_t i = 0U; i < (db->head_.page_size_ * BITSINBYTE); ++i )
  // {
  //  if( !(byte &= (1U << (i % BITSINBYTE))) )
  // 
  // }
  // //-----------------------------------------
  // lseek (db->hfile_, file_position, SEEK_SET);

  return offset;
}
//-------------------------------------------------------------------------------------------------------------
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
