#include "stdafx.h"
#include "mydb.h"
#include "mydb_block.h"

//-------------------------------------------------------------------------------------------------------------
int  main (void)
{
 sDBC conf = {0};
 conf.db_size = 700;
 conf.page_size = 100;

 sDB *mydb = dbcreate ("db.dat", &conf);
 if ( mydb )
 {
  uint_t k, d, sz = sizeof (int);
  
  k = 0xEEEEEEEE;
  d = 0xDDDDDDDD;
  db_insert (mydb, &k, sz, &d, sz);

  k = 0xCCCCCCCC;
  d = 0xDDDDDDDD;
  db_insert (mydb, &k, sz, &d, sz);

  k = 0xFFFFFFFF;
  d = 0x44444444;
  db_insert (mydb, &k, sz, &d, sz);

  k = 0xFFFFFFFF;
  d = 0xDDDDDDDD;
  db_insert (mydb, &k, sz, &d, sz);

  k = 0xBBBBBBBB;
  d = 0xDDDDDDDD;
  db_insert (mydb, &k, sz, &d, sz);

  k = 0xAAAAAAAA;
  d = 0xDDDDDDDD;
  db_insert (mydb, &k, sz, &d, sz);
 
  int *v = NULL;
  db_select (mydb, &k, sz, (void**) &v, &sz);

  printf ("%x\n%d", *v, sizeof (sDBHB));
 }
 db_close (mydb);

 return 0;
}
//-------------------------------------------------------------------------------------------------------------
