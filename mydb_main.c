#include "stdafx.h"
#include "mydb.h"

//-------------------------------------------------------------------------------------------------------------
/* A small example of the public API: create, insert, select, delete, reopen */
static void print_value(sDB *db, const char *key)
{
    void *val = NULL;
    size_t len = 0U;

    if (db_select(db, (void *)key, strlen(key), &val, &len) == 0)
        printf("  %-8s -> %.*s\n", key, (int)len, (const char *)val);
    else
        printf("  %-8s -> (not found)\n", key);
    free(val);
}
//-------------------------------------------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    const char *file = (argc > 1) ? argv[1] : "demo.db";

    sDBC conf = {0};
    conf.db_size = 16U * 1024U * 1024U;
    conf.page_size = 4U * 1024U;
    conf.cache_size = 1U * 1024U * 1024U;

    sDB *db = dbcreate(file, &conf);
    if (!db)
        return EXIT_FAILURE;

    const char *pairs[][2] = {
        {"apple", "red"}, {"banana", "yellow"}, {"cherry", "dark red"}, {"kiwi", "green"}};
    for (size_t i = 0U; i < sizeof(pairs) / sizeof(*pairs); ++i)
        db_insert(db, (void *)pairs[i][0], strlen(pairs[i][0]), (void *)pairs[i][1], strlen(pairs[i][1]));

    db_insert(db, "kiwi", 4U, "brown", 5U); /* update */
    db_delete(db, "banana", 6U);

    printf("max key+value size: %zu bytes\n", db_max_kv_size(db));
    printf("after insert/update/delete:\n");
    print_value(db, "apple");
    print_value(db, "banana");
    print_value(db, "kiwi");
    db_close(db);

    if (!(db = dbopen(file)))
        return EXIT_FAILURE;
    printf("after reopen:\n");
    print_value(db, "cherry");
    print_value(db, "kiwi");
    db_close(db);

    return EXIT_SUCCESS;
}
//-------------------------------------------------------------------------------------------------------------
