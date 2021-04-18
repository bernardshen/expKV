#ifndef _HOPSCOTCH_TABLE_H
#define _HOPSCOTCH_TABLE_H

#include <stdlib.h>
#include <stdint.h>
#include "base_table.h"
#include "spinlock.h"
#include "kvTypes.h"
#include "mm.h"

#define HOPSCOTCH_TABLE_SIZE 4096
#define HOPSCOTCH_TABLE_NEIGHBOUR  8

#define HOPSCOTCH_TABLE_ITEM_VALID(itemVec) ((itemVec >> 7) & 1)
#define HOPSCOTCH_TABLE_ITEM_KEYLEN(itemVec) ((itemVec >> 3) & 0xF)
#define HOPSCOTCH_TABLE_ITEM_VEC(valid, keylen) ((valid << 7) | (klen << 3))

typedef struct _HopscotchTableItem {
    uint8_t emptyCnt;                           // number of empty slots
    uint8_t itemVec;                            // a valid code vector indicate if the item is valid or not
    // 1: valid
    // 2: keylen
    // spinlock lock;                              // used to lock the item
    uint8_t hopInfo;                            // used to indicate neighbours
    char key[KV_KEYLEN_LIMIT];
    int64_t value[2];
} HopscotchTableItem;


typedef struct _HopscotchTable {
    HopscotchTableItem table[HOPSCOTCH_TABLE_SIZE];
    spinlock lock;
} HopscotchTable;

int initHopscotchTable(BaseTable * t);

int hopscotchTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen);
int hopscotchTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen);
int hopscotchTableDel(BaseTable * table, char * key, size_t klen);


#endif