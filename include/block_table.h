#ifndef BLOCK_TABLE_H
#define BLOCK_TABLE_H

#include <stdlib.h>
#include <stdint.h>
#include "base_table.h"
#include "spinlock.h"
#include "kvTypes.h"
#include "mm.h"

#define BLOCK_TABLE_SIZE 1024
#define BLOCK_TABLE_BLOCK_SIZE 16

#define BLOCK_TABLE_ITEM_VALID(itemVec) ((itemVec >> 7) & 1)
#define BLOCK_TABLE_ITEM_KEYLEN(itemVec) ((itemVec >> 3) & 0xF)
#define BLOCK_TABLE_ITEM_VEC(valid, keylen) ((valid << 7) | (klen << 3))

typedef struct _BlockTableItem {
    uint8_t emptyCnt;                           // number of empty slots
    uint8_t itemVec[BLOCK_TABLE_BLOCK_SIZE];    // a valid code vector indicate if the item is valid or not
    // 1: valid
    // 2: keylen
    spinlock lock;                              // used to lock the item
    char key[BLOCK_TABLE_BLOCK_SIZE][KV_KEYLEN_LIMIT];
    int64_t value[BLOCK_TABLE_BLOCK_SIZE][2];
    struct _BlockTableItem * next;  // point to the next item with same hashkey
} BlockTableItem;


typedef struct _BlockTable {
    BlockTableItem table[BLOCK_TABLE_SIZE];
} BlockTable;

int initBlockTable(BaseTable * t);

int blockTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen);
int blockTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen);
int blockTableDel(BaseTable * table, char * key, size_t klen);

#endif