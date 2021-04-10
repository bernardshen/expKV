#include "base_table.h"
#include "simple_table.h"
#include "mm.h"

int initTable(BaseTable * t, MemoryManager * mm, TableType type) {
    int ret = -1;

    // store memory manager
    t->mm = mm;
    
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