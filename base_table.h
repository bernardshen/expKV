#ifndef BASE_TABLE_H
#define BASE_TABLE_H
#include <stdlib.h>

#include "mm.h"
#include "kvTypes.h"

typedef struct _BaseTable {
    void * table;
    MemoryManager * mm;
    int (*put)(void * table, char * key, size_t klen, char * value, size_t vlen);
    int (*get)(void * table, char * key, size_t klen, char * value, size_t *vlen);
    int (*del)(void * table, char * key, size_t klen);
} BaseTable;

int initTable(BaseTable * t, MemoryManager * mm, TableType type);

#endif