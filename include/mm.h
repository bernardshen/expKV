#ifndef MM_H
#define MM_H

#include <stdlib.h>
#include <infiniband/verbs.h>
#include "spinlock.h"
#include "kvTypes.h"

#ifndef TEST
#define ITEM_POOL_SIZE 65536
#else
#define ITEM_POOL_SIZE 10
#endif

typedef struct _MemoryManager {
    void * tableAddr;
    void * itemPool;   // a pool of item;
    void * dataAddr;   // a pointer to the storage that stores key and value can be either dataAddr1 or dataAddr2

    // use two backup space for compaction
    void * dataAddr1;  // a pointer to the storage that stores KV can be used as backup
    void * dataAddr2;  // a pointer to the storage that stores KV can be used as backup

    // allocate new table (only allow once)
    char tableAllocated; // 1 = allocated; 0 = free

    // allocate new item
    uint16_t * freeItemList;
    char * bitMap;          // 1 = allocated; 0 = free
    size_t freeSize;        // number of free items

    // record the size of items and table
    size_t tableSize;
    size_t itemSize;
    TableType ttype;

    // mrs
    struct ibv_mr * tableMR;
    struct ibv_mr * itemPoolMR;
    char MRRegistered; // 1 = registered; 0 = not registered

    // for mutex
    spinlock lock;
} MemoryManager;


size_t itemAddr2Index(MemoryManager * mm, void * addr);

int initMM(MemoryManager * mm, TableType type);

void * MMAllocItem(MemoryManager * mm);
void * MMAllocTable(MemoryManager * mm);

void MMFreeItem(MemoryManager * mm, void * itemAddr);
void MMFreeTable(MemoryManager * mm, void * tableAddr);

int MMRegisterMR(MemoryManager * mm, struct ibv_pd * pd, int access);

#endif