#ifndef RPC_H
#define RPC_H

#include "cm.h"
#include "base_table.h"
#include "RPCMessages.h"

typedef struct _RPCServer {
    ConnectionManager cm;
    MemoryManager mm;
    BaseTable table;
} RPCServer;

#endif