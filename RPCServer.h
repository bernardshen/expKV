#ifndef RPC_H
#define RPC_H

#include "cm.h"
#include "base_table.h"
#include "RPCMessages.h"
#include "kvTypes.h"
#include <pthread.h>

#define KV_RPCSERVER_MAX_THREADS 128

typedef struct _RPCServerWorkerReq {
    ConnectionManager * cm;
    BaseTable * table;
    int64_t nodeId;
} RPCServerWorkerReq;

typedef struct _RPCServer {
    ConnectionManager cm;
    MemoryManager mm;
    BaseTable table;
    
    // record some critical threads
    pthread_t connectorThread;
    pthread_t dispatcherThread;

    // record all the threads generated
    pthread_t threads[KV_RPCSERVER_MAX_THREADS];
    uint64_t  numThreads;
} RPCServer;

// called by the server to init itself
// create a connector thread
// create a dispatcher thread
int initRPCServer(RPCServer * rpcServer, TableType tableType);


#endif