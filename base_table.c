#include "base_table.h"

int initTable(BaseTable * t, TableType type) {
    int ret = -1;
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