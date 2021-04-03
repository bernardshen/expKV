#ifndef MM_H
#define MM_H

#include <stdlib.h>
#include "base_table.h"

typedef struct _MemoryManager {
    void * tableAddr;
    void * itemPool;   // a pool of item;
    void * dataAddr;   // a pointer to the storage that stores key and value can be either dataAddr1 or dataAddr2

    // use two backup space for compaction
    void * dataAddr1;  // a pointer to the storage that stores KV can be used as backup
    void * dataAddr2;  // a pointer to the storage that stores KV can be used as backup

    // allocate new item
    void * freeItemList;
    void * usedItemList;
    size_t freeSize;
    size_t usedSize;

    // record the size of items and table
    size_t tblSize;
    size_t itemSize;
    TableType ttype;
} MemoryManager;


int itemAddr2Index(MemoryManager * mm, void * addr);

int initMM(MemoryManager * mm, TableType type);
void * MMAllocItem();
void MMFreeitem(void * itemAddr);

#endif