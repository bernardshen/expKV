#include "mm.h"
#include "simple_table.h"
#include "base_table.h"
#include "testUtil.h"
#include <stdio.h>
#include <stdlib.h>

static void printMMInfo(MemoryManager * mm) {
    printf("---- MMInfo ----\n");
    printf("tableAddr: 0x%08x\n", mm->tableAddr);
    printf("itemPool: 0x%08x\n", mm->itemPool);
    printf("tableAllocated: %s\n", mm->tableAllocated);
    printf("freeSize: %d\n", mm->freeSize);
    printf("MRRegistered: %d\n", mm->MRRegistered);
    printf("----------------\n");
}

static void printItem(void * item) {
    printf("Item: 0x%08x\n", item);
}

int main() {
    MemoryManager mm;
    int ret = -1;
    void * item;
    void * itemList[ITEM_POOL_SIZE + 1];
    // check initMM
    testName("Check InitMM");
    ret = initMM(&mm, SIMPLE);
    checkErr(ret, "initMM");
    printMMInfo(&mm);

    // check single alloc
    testName("Check Single Alloc");
    item = MMAllocItem(&mm);
    printItem(item);
    printMMInfo(&mm);

    // check single free
    testName("Check Single Free");
    MMFreeItem(&mm, item);
    printMMInfo(&mm);

    // check double free
    testName("Check Double Free");
    MMFreeItem(&mm, item);
    printMMInfo(&mm);

    // check multiple allocate
    testName("Check Multiple Alloc");
    for (int i = 0; i < ITEM_POOL_SIZE; i ++) {
        itemList[i] = MMAllocItem(&mm);
        printMMInfo(&mm);
    }

    // check over alloc
    testName("Check Over-Alloc 1");
    item = MMAllocItem(&mm);
    printItem(item);
    printMMInfo(&mm);

    // check free NULL
    testName("Check Free NULL");
    MMFreeItem(&mm, item);
    printMMInfo(&mm);

    // check single free after full
    testName("Check Single Free After Full");
    MMFreeItem(&mm, itemList[0]);
    printMMInfo(&mm);

    // check single alloc after full
    testName("Check Single Alloc After Full");
    printItem(itemList[0]);
    itemList[0] = MMAllocItem(&mm);
    printItem(itemList[0]);

    //check over alloc
    testName("Check Over-Alloc 2");
    item = MMAllocItem(&mm);
    printItem(item);

    // check multiple free
    testName("Check Multiple Free");
    for (int i = 0; i < ITEM_POOL_SIZE; i++) {
        MMFreeItem(&mm, itemList[i]);
    }
    printMMInfo(&mm);

    // check single alloc and single free after clear
    testName("Check Single Alloc and Single Free after Clear");
    item = MMAllocItem(&mm);
    printItem(item);
    MMFreeItem(&mm, item);
    printMMInfo(&mm);

    // check over free
    testName("Check Over-Free after Clear");
    MMFreeItem(&mm, item);
    printMMInfo(&mm);
    return 0;
}