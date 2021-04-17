#include "base_table.h"
#include "simple_table.h"
#include "block_table.h"
#include "cuckoo_table.h"
#include "mm.h"

int initTable(BaseTable * t, MemoryManager * mm, TableType type) {
    int ret = -1;

    // store memory manager
    t->mm = mm;
    
    // init table
    switch (type) {
    case SIMPLE:
        ret = initSimpleTable(t);
        if (ret < 0) {
            printf("initSimpleTable failed\n");
        }
        break;
    
    case BLOCK:
        ret = initBlockTable(t);
        if (ret < 0) {
            printf("initBlockTable failed\n");
        }
        break;
    
    case CUCKOO:
        ret = initCuckooTable(t);
        if (ret < 0) {
            printf("initCuckooTable failed\n");
        }
        break;

    default:
        ret = -1;
    }
    return ret;
}