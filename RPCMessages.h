#ifndef RPC_MESSAGES_H
#define RPC_MESSAGES_H

#include "kvTypes.h"
#include <stdint.h>
#include <stdlib.h>

struct _RPCMessage {
    ReqType reqType;
    uint64_t  nodeId;
    char    key[KV_KEYLEN_LIMIT];
    int64_t value;
} __attribute__((packed));

typedef struct _RPCMessage RPCMessage;

#endif