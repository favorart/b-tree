#include "stdafx.h"
#include "mydb.h"


//-------------------------------------------------------------------------------------------------------------
int main ()
{
 sDBC conf = {0};
 conf.db_size = 2000;
 conf.page_size = 100;

 sDB *mydb = db_open ("db.dat", &conf);
 if ( mydb )
 {
  uint_t k, d, sz = sizeof (int);
  
  k = 0xFFFFFFFF;
  d = 0xDDDDDDDD;
  db_put (mydb, &k, sz, &d, sz);

  k = 0xEEEEEEEE;
  d = 0xDDDDDDDD;
  db_put (mydb, &k, sz, &d, sz);
 
  uint_t* v = NULL;
  db_get (mydb, k, sz, &v, &sz);
 }
 db_cls (mydb);

 return 0;
}
//-------------------------------------------------------------------------------------------------------------
