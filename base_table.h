#ifndef BASE_TABLE_H
#define BASE_TABLE_H
#include <stdlib.h>

typedef enum _TableType {
    SIMPLE,
    COCKOO,
    HOPSCOTCH,
} TableType;

typedef struct _BaseTable {
    void * table;
    int (*put)(void * table, char * key, size_t klen, char * value, size_t vlen);
    int (*get)(void * table, char * key, size_t klen, char * value, size_t *vlen);
    int (*del)(void * table, char * key, size_t klen);
} BaseTable;

int initTable(BaseTable * t, TableType type);

#endif