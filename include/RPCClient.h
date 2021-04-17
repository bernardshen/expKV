#ifndef RPC_CLIENT_H
#define RPC_CLIENT_H

#include "cm.h"
#include "kvTypes.h"

#ifdef __cplusplus
extern "C"
{
#endif
typedef struct _RPCClient {
    ConnectionManager cm;
    TableType tableType;
} RPCClient;

int initRPCClient(RPCClient * rpcClient, char * host, TableType tableType);
int RPCClientKVPut(RPCClient * rpcClient, char * key, uint64_t klen, void * value, uint64_t vlen);
int RPCClientKVDel(RPCClient * rpcClient, char * key, uint64_t klen);
int RPCClientKVGet2S(RPCClient * rpcClient, char * key, uint64_t klen, __out void * value, __out uint64_t * vlen);
int RPCClientKVGet1S(RPCClient * rpcClient, char * key, uint64_t klen, __out void * value, __out uint64_t * vlen);

#ifdef __cplusplus
}
#endif


#endif