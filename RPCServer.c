#include "RPCServer.h"
#include "kvTypes.h"
#include "base_table.h"
#include "cm.h"
#include "mm.h"
#include "RPCMessages.h"
#include "utils.h"
#include <assert.h>

// only this thread will alter peers in the CM
static void RPCServerConnectorThread(void * cm) {
    ConnectionManager * _cm = (ConnectionManager *)cm;
    CMServerConnect(_cm);   // loop forever never return
}

static int workerServeReply(RPCServerWorkerReq * workerReq, int32_t success) {
    // need to post recv before post send
    // client can only post one request at a time
    // client will not post another send before receiving from the server
    // -> the address can be same
    int ret = -1;
    
    // get peer
    PeerData * peer = workerReq->cm->peers[workerReq->nodeId];
    // get cm
    ConnectionManager * cm = workerReq->cm;

    // postRecv
    ret = CMPostRecv(cm, workerReq->nodeId);
    if (ret < 0) {
        printf("CMPostRecv failed\n");
        return -1;
    }

    // prepare reply
    RPCReply reply;
    reply.success = htonl(success);

    // TODO: use inline send
    // set reply to the same buffer
    // ** no send request will come to this buffer **
    memcpy(peer->mr->addr, &reply, sizeof(RPCReply));
    
    // postSend
    ret = CMPostSend(cm, peer, &reply, sizeof(reply));
    if (ret < 0) {
        printf("CMPostSend failed\n");
        return -1;
    }
}

static int workerServePut(BaseTable * table, RPCMessage * msg) {
    int ret = -1;
    
    // get k, v
    char key[KV_KEYLEN_LIMIT];
    int64_t value;
    uint64_t klen;
    uint64_t vlen;

    memcpy(key, msg->key, KV_KEYLEN_LIMIT);
    value = msg->value;
    klen = msg->klen;
    vlen = msg->vlen;

    ret = table->put(table, key, klen, &value, vlen);
    if (ret < 0) {
        printf("table->put failed\n");
        return -1;
    }
    return 0; // return success here
}

static workerDel(BaseTable * table, RPCMessage * msg) {
    int ret = -1;

    // get k, v
    char key[KV_KEYLEN_LIMIT];
    uint64_t klen;

    memcpy(key, msg->key, KV_KEYLEN_LIMIT);
    klen = msg->klen;

    ret = table->del(table, key, klen);
    if (ret < 0) {
        printf("table->del failed\n");
        return -1;
    }
    return 0; // return success here
}

static void RPCServerWorker(void * _workerReq) {
    // convert data structure
    int ret = -1;
    RPCServerWorkerReq * workerReq = (RPCServerWorkerReq *)_workerReq;
    
    // get peer
    PeerData * peer = workerReq->cm->peers[workerReq->nodeId];
    
    // get msg
    RPCMessage * tmpMsg = (RPCMessage *)(peer->mr->addr);
    RPCMessage msg;
    // deserialize
    memset(&msg, 0, sizeof(RPCMessage));
    msg.reqType = ntohl(tmpMsg->reqType);
    msg.nodeId = ntohll(tmpMsg->nodeId);
    memcpy(&msg.key, &(tmpMsg->key), KV_KEYLEN_LIMIT);
    msg.value = ntohll(tmpMsg->value);
    msg.klen = ntohll(tmpMsg->klen);
    msg.vlen = ntohll(tmpMsg->vlen);

    // serve req
    // ret should be 0 on success after this block of code
    switch (msg.reqType) {
    case PUT:
        ret = workerServePut(workerReq->table, &msg);
        if (ret < 0) {
            printf("workerPut failed\n");
        }
        break;
    case DEL:
        ret = workerServeDel(workerReq->table, &msg);
        if (ret < 0) {
            printf("workerDel failed\n");
        }
    default:
        ret = -1;
    }   

    // reply to client
    ret = workerServeReply(workerReq, ret);
    if (ret < 0) {
        printf("workerServeReply failed\n");
    }

    free(_workerReq);
    return ret; // 0 - success, -1 - fail
}

static void RPCServerDispatcher(void * _rpcServer) {
    RPCServer * rpcServer = (RPCServer *)_rpcServer;
    int c = 0;
    while (1) {
        uint64_t nodeId = -1;
        c = CMPollOnce(&(rpcServer->cm), &nodeId);
        if (c < 0) {
            // some error happen when polling cq 
            printf("CMPollOnce failed\n");
        } else if (c == 0) {
            // no completion
            
        } else {
            assert(c == 1);
            // the completion is a completion of server itself
            if (nodeId == -1) {
                continue;
            }

            // create RPCServerWorkerReq
            RPCServerWorkerReq * workerReq = (RPCServerWorkerReq *)malloc(sizeof(RPCServerWorkerReq));
            workerReq->cm = &(rpcServer->cm);
            workerReq->table = &(rpcServer->table);
            workerReq->nodeId = nodeId;

            // create a new thread
            pthread_t tid = 0;
            int ret = pthread_create(&tid, NULL, RPCServerWorker, workerReq);
            if (ret < 0) {
                printf("Failed to generate worker thread\n");
                free(nodeId);
            } else {
                // add the thread to the rpcServer
                rpcServer->threads[rpcServer->numThreads] = tid;
                rpcServer->numThreads += 1;
            }
        }
    }
}

// init all the data structures and create a connector thread
int initRPCServer(RPCServer * rpcServer, TableType tableType) {
    int ret = -1;
    
    // initialize counters
    rpcServer->numThreads = 0;

    // initialize mm **Need to be initialized first**
    // BaseTable and CM depends on MM
    ret = initMM(&(rpcServer->mm), tableType);
    if (ret < 0) {
        printf("initMM failed\n");
        return -1;
    }

    // initialize table
    ret = initTable(&(rpcServer->table), &(rpcServer->mm), SIMPLE);
    if (ret < 0) {
        printf("initTable failed\n");
        return -1;
    }

    // initialize CM
    ret = initCM(&(rpcServer->cm), SERVER);
    if (ret < 0) {
        printf("initCM failed\n");
        return -1;
    }

    // register mr
    ret = CMServerRegisterMR(&(rpcServer->cm), &(rpcServer->mm));
    if (ret < 0) {
        printf("CMRegisterMR failed\n");
        return -1;
    }

    // start connector thread
    printf("RPCServer: Starting connecting thread\n");
    ret = pthread_create(&(rpcServer->connectorThread), NULL, RPCServerConnectorThread, (void *)&(rpcServer->cm));
    if (ret < 0) {
        printf("RPCServerConnectorThread failed\n");
        return -1;
    }

    // start dispatcher thread
    printf("RPCServer: Starting dispatcher thread\n");
    ret = pthread_create(&(rpcServer->dispatcherThread), NULL, RPCServerDispatcher, (void *)rpcServer);
    if (ret < 0) {
        printf("RPCServerDispatcher failed\n");
        return -1;
    }

    return 0;
}

