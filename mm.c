#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <infiniband/verbs.h>
#include "mm.h"
#include "base_table.h"
#include "simple_table.h"
#include "spinlock.h"

///// Private functions
static int getSizes(TableType type, size_t * tblSize, size_t * itemSize) {
    switch (type) {
    case SIMPLE:
        *tblSize = sizeof(SimpleTable);
        *itemSize = sizeof(SimpleTableItem);
        return 0;
    
    default:
        return -1;
    }
}

///// Public functions
size_t itemAddr2Index(MemoryManager * mm, void * addr) {
    uint64_t offset = (uint64_t)addr - (uint64_t)(mm->itemPool);
    assert((offset % (mm->itemSize)) == 0);
    return (size_t)offset / (mm->itemSize);
}

void * itemIndex2Addr(MemoryManager * mm, size_t index) {
    assert(index < ITEM_POOL_SIZE);
    uint64_t offset = index * (mm->itemSize);
    uint64_t addr = (uint64_t)(mm->itemPool) + offset;
    return (void *)addr;
}

int initMM(MemoryManager * mm, TableType type) {
    int tableSize, itemSize;
    void * tableAddr;
    void * itemPool;
    int ret = -1;

    // get table size and item size
    ret = getSizes(type, &tableSize, &itemSize);
    if (ret < 0) {
        return ret;
    }

    // allocate memory pool
    tableAddr = malloc(tableSize);
    itemPool = malloc(itemSize * ITEM_POOL_SIZE);
    if (tableAddr == NULL || itemPool == NULL) {
        return -1;
    }

    // lock the structure
    spin_lock(&(mm->lock));

    // set mm data
    mm->tableAddr = tableAddr;
    mm->itemPool = itemPool;
    mm->tableSize = tableSize;
    mm->itemSize = itemSize;
    mm->ttype = type;
    mm->tableAllocated = 0;
    spin_unlock(&(mm->lock));

    // initialize free list and alloc list
    mm->freeItemList = (uint16_t *)malloc(sizeof(uint16_t) * ITEM_POOL_SIZE);
    mm->bitMap = (char *)malloc(sizeof(char) * ITEM_POOL_SIZE);
    mm->freeSize = ITEM_POOL_SIZE;
    for (int i = 0; i < ITEM_POOL_SIZE; i++) {
        mm->freeItemList[i] = (uint16_t)i;
        mm->bitMap[i] = 0;
    }

    // unlock the item
    spin_unlock(&(mm->lock));
    return 0;
}

void * MMAllocItem(MemoryManager * mm) {
    void * ret = NULL;
    
    // lock the memory manager
    spin_lock(&(mm->lock));

    // check if there is still free item
    if (mm->freeSize == 0) {
        spin_unlock(&(mm->lock));
        return NULL;
    }

    // get a free item
    size_t itemId = mm->freeItemList[mm->freeSize - 1];
    mm->freeSize --;
    // set the bitMap
    assert(mm->bitMap[itemId] == 0);
    mm->bitMap[itemId] = 1;
    // get retval
    ret = itemIndex2Addr(mm, itemId);

    // unlock
    spin_unlock(&(mm->lock));
    return ret;
}

void * MMAllocTable(MemoryManager * mm) {
    void * ret = NULL;
    spin_lock(&(mm->lock));

    // check if allocated
    if (mm->tableAllocated == 0) {
        mm->tableAllocated = 1;
        ret = mm->tableAddr;
    }
    
    // unlock the item and return
    spin_unlock(&(mm->lock));
    return ret;
}

void MMFreeItem(MemoryManager * mm, void * itemAddr) {
    // if it is an empty item
    if (itemAddr == NULL) {
        return;
    }

    spin_lock(&(mm->lock));
    size_t itemId = itemAddr2Index(mm, itemAddr);
    
    // check if the item is allocated
    if (mm->bitMap[itemId] == 1) {
        mm->freeItemList[mm->freeSize] = itemId;
        mm->freeSize ++;
        mm->bitMap[itemId] = 0;
    }
    spin_unlock(&(mm->lock));
    return;
}

void MMFreeTable(MemoryManager * mm, void * tableAddr) {
    spin_lock(&(mm->lock));
    assert(mm->tableAddr == tableAddr);

    mm->tableAllocated = 0;
    mm->freeSize = ITEM_POOL_SIZE;
    for (int i = 0; i < ITEM_POOL_SIZE; i++) {
        mm->freeItemList[i] = (uint16_t)i;
        mm->bitMap[i] = 0;
    }
    spin_unlock(&(mm->lock));
    return;
}

int MMRegisterMR(MemoryManager * mm, struct ibv_pd * pd, int access) {
    spin_lock(&(mm->lock));

    // check if mr is registered
    if (mm->MRRegistered == 1) {
        spin_unlock(&(mm->lock));
        return 0;
    } 

    // register the whole table
    struct ibv_mr * tableMR;
    struct ibv_mr * itemPoolMR;
    tableMR = ibv_reg_mr(pd, mm->tableAddr, mm->tableSize, access);
    itemPoolMR = ibv_reg_mr(pd, mm->itemPool, (mm->itemSize) * ITEM_POOL_SIZE, access);

    if (tableMR == NULL || itemPoolMR == NULL) {
        spin_unlock(&(mm->lock));
        return -1;
    }

    // modify the indicator to show mr is registerd
    mm->MRRegistered = 1;

    // unlock and return success
    spin_unlock(&(mm->lock));
    return 0;
}