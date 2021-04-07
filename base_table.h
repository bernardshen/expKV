#ifndef BASE_TABLE_H
#define BASE_TABLE_H
#include <stdlib.h>

#include "mm.h"
#include "kvTypes.h"

// TODO: store network format of data in the table

typedef struct _BaseTable {
    void * table;
    MemoryManager * mm;
    int (*put)(struct _BaseTable * table, char * key, size_t klen, char * value, size_t vlen);
    int (*get)(struct _BaseTable * table, char * key, size_t klen, char * value, size_t *vlen);
    int (*del)(struct _BaseTable * table, char * key, size_t klen);
} BaseTable;

int initTable(BaseTable * t, MemoryManager * mm, TableType type);

#endif