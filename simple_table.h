#ifndef SIMPLE_TABLE_H
#define SIMPLE_TABLE_H

#include <stdlib.h>
#include <stdint.h>
#include "base_table.h"
#include "spinlock.h"
#include "kvTypes.h"
#include "mm.h"

#define SIMPLE_TABLE_SIZE 256

#define SIMPLE_TABLE_ITEM_VALID(itemVec) ((itemVec >> 7) & 1)
#define SIMPLE_TABLE_ITEM_KEYLEN(itemVec) ((itemVec >> 3) & 0xF)
#define SIMPLE_TABLE_ITEM_VEC(valid, keylen) ((valid << 7) | (klen << 3))

typedef struct _SimpleTableItem {
    uint8_t itemVec;                      // indicate if the item is valid or not
    // 1: valid
    // 2: keylen
    spinlock lock;                         // used to lock the item
    char key[KV_KEYLEN_LIMIT];
    int64_t value[2];
    struct _SimpleTableItem * next;  // point to the next item with same hashkey
} SimpleTableItem;


typedef struct _SimpleTable {
    SimpleTableItem table[SIMPLE_TABLE_SIZE];
} SimpleTable;

int initSimpleTable(BaseTable * t);

int simpleTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen);
int simpleTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen);
int simpleTableDel(BaseTable * table, char * key, size_t klen);

#endif