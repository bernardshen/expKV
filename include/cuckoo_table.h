#ifndef CUCKOO_TABLE_H
#define CUCKOO_TABLE_H

#include <stdlib.h>
#include <stdint.h>
#include "base_table.h"
#include "spinlock.h"
#include "kvTypes.h"
#include "mm.h"

#define CUCKOO_TABLE_SIZE 1024
#define CUCKOO_TABLE_NUM  2

#define CUCKOO_TABLE_ITEM_VALID(itemVec) ((itemVec >> 7) & 1)
#define CUCKOO_TABLE_ITEM_KEYLEN(itemVec) ((itemVec >> 3) & 0xF)
#define CUCKOO_TABLE_ITEM_VEC(valid, keylen) ((valid << 7) | (klen << 3))

typedef struct _CuckooTableItem {
    uint8_t emptyCnt;                           // number of empty slots
    uint8_t itemVec;                            // a valid code vector indicate if the item is valid or not
    // 1: valid
    // 2: keylen
    spinlock lock;                              // used to lock the item
    char key[KV_KEYLEN_LIMIT];
    int64_t value[2];
    struct _CuckooTableItem * next;  // point to the next item with same hashkey
} CuckooTableItem;


typedef struct _CuckooTable {
    CuckooTableItem table[CUCKOO_TABLE_NUM][CUCKOO_TABLE_SIZE];
} CuckooTable;

int initCuckooTable(BaseTable * t);

int cuckooTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen);
int cuckooTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen);
int cuckooTableDel(BaseTable * table, char * key, size_t klen);

uint64_t cuckoo_hash(int version, char * key, size_t klen);

#endif