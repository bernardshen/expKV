#include "RPCClient.h"
#include "kvTypes.h"
#include "utils.h"
#include "cm.h"
#include "simple_table.h"
#include <assert.h>

// ==== private functions ====
static int clientWaitReply(ConnectionManager * cm, __out void ** reply) {
    while (1) {
        int64_t nodeId;
        int c;
        c = CMPollOnce(cm, &nodeId);
        if (c == -1) {
            printf("CMPollCQFailed\n");
            return -1;
        } else if (c == -2) {
            printf("Work Completion Failed\n");
            return -1; 
        } else if (c == 0){
            continue;
        } else {
            assert(c == 1);
            if (nodeId == -1) {
                // post send completion
                continue;
            } else if (nodeId == -2) {
                // rdma read
                *reply = cm->peers[0]->mr->addr;
                return 0;
            } else {
                // send
                *reply = cm->peers[nodeId]->mr->addr;
                return 0;
            }
        }
    }
}

static int simpleTableRDMAReadTable(ConnectionManager * cm, char * key, uint64_t klen, __out SimpleTableItem ** item) {
    int ret = -1;
    
    // get key hash and calculate address
    uint64_t keyhash = hash(key, klen) % SIMPLE_TABLE_SIZE;
    uint64_t offset = keyhash * sizeof(SimpleTableItem);
    uintptr_t remoteAddr = (cm->peers[0]->tableAddr) + offset;

    // post read
    ret = CMReadTable(cm, 0, remoteAddr, sizeof(SimpleTableItem));
    if (ret < 0) {
        printf("CMReadTable failed\n");
        return -1;
    }

    // get reply
    ret = clientWaitReply(cm, (void **)item);
    if (ret < 0) {
        printf("clientWaitReply failed\n");
        return -1;
    }
    return 0; // return success here
}

static int simpleTableRDMAReadItem(ConnectionManager * cm, uintptr_t itemAddr, __out SimpleTableItem ** item) {
    int ret = -1;
    
    // post read
    ret = CMReadItemPool(cm, 0, itemAddr, sizeof(SimpleTableItem));
    if (ret < 0) {
        printf("CMReadItemPool failed\n");
        return -1;
    }

    // get reply
    ret = clientWaitReply(cm, (void **)item);
    if (ret < 0) {
        printf("clientWaitReply failed\n");
        return -1;
    }
    return 0; // return success here
}

static int simpleTableRemoteGet(RPCClient * rpcClient, char * key, uint64_t klen, __out void * value, __out uint64_t * vlen) {
    int ret = -1;
    ConnectionManager * cm = &(rpcClient->cm);

    // get the first item
    SimpleTableItem * item = NULL;
    ret = simpleTableRDMAReadTable(cm, key, klen, &item);
    if (ret < 0) {
        printf("simpleTableRDMAReadTable failed\n");
        return -1;
    }

    // conduct remote linked list traversal
    if (SIMPLE_TABLE_ITEM_VALID(item->itemVec)) {
        // deal with the first item because its in the table mr !!!! shit
        size_t keylen = SIMPLE_TABLE_ITEM_KEYLEN(item->itemVec);
        if (compare_key(item->key, keylen, key, klen)) {
            while (item->value[1] != hash_md5((const uint8_t *)&(item->value[0]), sizeof(int64_t))) {
                ret = simpleTableRDMAReadTable(cm, key, klen, &item);
                if (ret < 0) {
                    printf("simpleTableRDMAReadTable failed\n");
                    return -1;
                }
            }
            *(int64_t *)value = item->value[0];
            *vlen = sizeof(int64_t);
            return 0;
        }

        // deal with the remaining items
        SimpleTableItem * lp;       // local pointer: point to the data structure in local memory
        SimpleTableItem * rp;       // remote pointer: point to the data structure in remote memory
        for (rp = item->next; rp; rp = rp->next) {
            // fetch remote item if not the first item
            ret = simpleTableRDMAReadItem(cm, (uintptr_t)rp, &lp);
            if (ret < 0) {
                printf("simpleTableRDMAReadItem failed\n");
                return -1;
            }
            // now lp points to the remote item
            // if key matches
            size_t keylen = SIMPLE_TABLE_ITEM_KEYLEN(lp->itemVec);
            if (compare_key(lp->key, keylen, key, klen)) {
                // check if the md5 matches
                // if not, constantly fetching remote item
                while (lp->value[1] != hash_md5((const uint8_t *)&(lp->value[1]), sizeof(int64_t))) {
                    ret = simpleTableRDMAReadItem(cm, rp, &lp);
                    if (ret < 0) {
                        printf("simpleTableRDMAReadItem failed\n");
                        return -1;
                    }
                }
                // find the safe value
                *(int64_t *)value = lp->value[0];
                *vlen = sizeof(int64_t);
                return 0;
            }
        }
    }   // the item itself is not valid
    
    return -1; // return fail here
}


// ==== public functions ====
int initRPCClient(RPCClient * rpcClient, TableType tableType) {
    int ret = -1;

    // set rpcClient data
    rpcClient->tableType = tableType;

    // initCM
    printf("RPCClient: initCM\n");
    ret = initCM(&(rpcClient->cm), CLIENT);
    if (ret < 0) {
        printf("initCM failed\n");
        return -1;
    }

    // connect to the server
    printf("RPCClient: connecting to the server\n");
    ret = CMClientConnect(&(rpcClient->cm));
    if (ret < 0) {
        printf("CMClientConnect failed\n");
        return -1;
    }

    return 0; // return success here
}

int RPCClientKVPut(RPCClient * rpcClient, char * key, uint64_t klen, void * value, uint64_t vlen) {
    int ret = -1;
    ConnectionManager * cm = &(rpcClient->cm);

    // prepare request
    RPCRequest request;
    memset(&request, 0, sizeof(request));
    request.reqType = htonl(PUT);
    memcpy(request.key, key, klen);
    request.klen = htonll(klen);
    request.value = htonll(*(int64_t *)value);
    request.vlen = htonll(sizeof(int64_t));
    request.nodeId = htonll(cm->peers[0]->peerId);

    // post recv first
    ret = CMPostRecv(cm, 0);
    if (ret < 0) {
        printf("CMPostRecv failed\n");
        return -1;
    }

    // post send and wait reply
    ret = CMPostSend(cm, 0, &request, sizeof(RPCRequest));
    if (ret < 0) {
        printf("CMPostSend failed\n");
        return -1;
    }

    // wait for reply
    // will block forever
    RPCReply reply;
    RPCReply *tmpReply;
    memset(&reply, 0, sizeof(RPCReply));
    ret = clientWaitReply(cm, &tmpReply);
    if (ret < 0) {
        printf("clientWaitReply failed\n");
        return -1;
    }
    
    // de-serialize the reply
    reply.success = ntohl(tmpReply->success);
    return reply.success;
}

int RPCClientKVDel(RPCClient * rpcClient, char * key, uint64_t klen) {
    int ret = -1;
    ConnectionManager * cm = &(rpcClient->cm);

    // prepare request
    RPCRequest request;
    memset(&request, 0, sizeof(request));
    request.reqType = htonl(DEL);
    memcpy(request.key, key, klen);
    request.klen = htonll(klen);

    // post recv first
    ret = CMPostRecv(cm, 0);
    if (ret < 0) {
        printf("CMPostRecv failed\n");
        return -1;
    }

    // post send and wait reply
    ret = CMPostSend(cm, 0, &request, sizeof(RPCRequest));
    if (ret < 0) {
        printf("CMPostSend failed\n");
        return -1;
    }

    // wait for reply
    // may block forever
    RPCReply reply;
    memset(&reply, 0, sizeof(RPCReply));
    ret = clientWaitReply(cm, &reply);
    if (ret < 0) {
        printf("clientWaitReply failed\n");
        return -1;
    }
    
    // de-serialize the reply
    reply.success = ntohl(reply.success);
    return reply.success;
}


int RPCClientKVGet2S(RPCClient * rpcClient, char * key, uint64_t klen, __out void * value, __out uint64_t * vlen) {
    int ret = -1;
    ConnectionManager * cm = &(rpcClient->cm);

    // prepare request
    RPCRequest request;
    memset(&request, 0, sizeof(RPCRequest));
    request.reqType = htonl(GET);
    memcpy(request.key, key, klen);
    request.klen = htonll(klen);

    // post recv first
    ret = CMPostRecv(cm, 0);
    if (ret < 0) {
        printf("CMPostRecv failed\n");
        return -1;
    }

    // post send and wait reply
    ret = CMPostSend(cm, 0, &request, sizeof(RPCRequest));
    if (ret < 0) {
        printf("CMPostSend failed\n");
        return -1;
    }

    // wait for reply
    // may block forever
    RPCReply reply;
    RPCReply * tmpReply;
    memset(&reply, 0, sizeof(RPCReply));
    ret = clientWaitReply(cm, &tmpReply);
    if (ret < 0) {
        printf("clientWaitReply failed\n");
        return -1;
    }

    // de-serizalize
    reply.success = ntohl(tmpReply->success);
    reply.value = ntohll(tmpReply->value);
    reply.vlen = ntohll(tmpReply->vlen);

    *(int64_t *)value = reply.value;
    *vlen = reply.vlen;
    return reply.success;
}


int RPCClientKVGet1S(RPCClient * rpcClient, char * key, uint64_t klen, __out void * value, __out uint64_t * vlen) {
    int ret = -1;

    // dispatch different remote one-sided get methods
    switch (rpcClient->tableType) {
    case SIMPLE:
        ret = simpleTableRemoteGet(rpcClient, key, klen, value, vlen);
        if (ret < 0) {
            printf("simpleTableRemoteGet failed\n");
        }
        break;
    default:
        break;
    }

    return ret; // 0 - success; -1 - fail
}