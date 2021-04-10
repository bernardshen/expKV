#ifndef RPC_MESSAGES_H
#define RPC_MESSAGES_H

#include "kvTypes.h"
#include <stdint.h>
#include <stdlib.h>

struct _RPCRequest {
    ReqType     reqType;
    uint64_t    nodeId;
    char        key[KV_KEYLEN_LIMIT];
    uint64_t    klen;
    int64_t     value;
    uint64_t    vlen;
} __attribute__((packed));

typedef struct _RPCRequest RPCRequest;

struct _RPCReply {
    int32_t    success; // 0 - success, -1 - fail
    int64_t    value;   // the value for get operation
    uint64_t   vlen;    // the length of the value
} __attribute__((packed));

typedef struct _RPCReply RPCReply;

#endif