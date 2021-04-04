#include "base_table.h"
#include "simple_table.h"
#include "mm.h"

int initTable(BaseTable * t, TableType type) {
    int ret = -1;

    // init memory manager
    t->mm = (MemoryManager *)malloc(sizeof(MemoryManager));
    initMM(t->mm, type);
    
    // init table
    switch (type)
    {
    case SIMPLE:
        ret = initSimpleTable(t);
        break;
    
    default:
        ret = -1;
    }
    return ret;
}