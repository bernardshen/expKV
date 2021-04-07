#ifndef CM_H
#define CM_H

#include <infiniband/verbs.h>
#include <stdint.h>
#include "kvTypes.h"
#include "spinlock.h"
#include "mm.h"
#include "RPCMessages.h"

#define KV_CONN_PORT 2333
#define KV_PEER_NUM 512

// TODO: Separate send and recv cq for optimization

typedef struct _PeerData {
    // data on both server and client side
    struct ibv_qp * qp; // the qp for communication
    struct ibv_mr * mr; // the mr for send/recv only local access
    int64_t peerId;      // the id of the peer

    // data on client side
    uint64_t tableAddr;
    uint64_t itemPoolAddr;
    uint32_t tableRKey;
    uint32_t itemPoolRKey;
    int64_t  remotePeerId;
} PeerData;


struct _ConData {
    uint64_t tableAddr;
    uint32_t tableRKey;
    uint64_t itemPoolAddr;
    uint32_t itemPoolRKey;
    uint32_t qpNum;
    uint16_t lid;
    uint8_t gid[16];
} __attribute__((packed));

typedef struct _ConData ConData;


typedef struct _ConnectionManager {
    // ---- shared for server and clients ----
    int sock;                       // for client connection
    struct ibv_context * ctx;       // ibv_device
    struct ibv_pd * pd;             // share pd
    struct ibv_cq * cq;             // share cq
    struct ibv_port_attr portAttr;  // share portAttr
    // ---- only cq poller/client will alter cq structure ----
    // -> no need for lock

    // peer information **Append-only**
    // only RPCServer Connector and a single client thread will alter this data
    // -> no need for lock
    PeerData * peers[KV_PEER_NUM];              // record peer data
    size_t peerNum;                             // record peer number

    NodeType nodeType;

    // ---- only used by the server ----
    struct ibv_mr * tableMR;
    struct ibv_mr * itemPoolMr;
} ConnectionManager;


int initCM(ConnectionManager * cm, NodeType nodeType);

// register should be called before CMServerConnect
int CMServerRegisterMR(ConnectionManager * cm, MemoryManager * mm);

// server accept client requests
void CMServerConnect(ConnectionManager * cm);

// client connect to servers
int CMClientConnect(ConnectionManager * cm);

// a wrapper for post recv
int CMPostRecv(ConnectionManager * cm, int64_t peerId);
// a wrapper for post send
int CMPostSend(ConnectionManager * cm, int64_t nodeId, void * message, size_t mlen);

// a wrapper for poll cq
int CMPollOnce(ConnectionManager * cm, __out int64_t * nodeId);

// a wrapper for remote table access
int CMReadTable(ConnectionManager * cm, int64_t nodeId, void * addr, uint64_t len);

// a wrapper for remote itempool access
int CMReadItemPool(ConnectionManager * cm, int64_t nodeId, void * addr, uint64_t len);

#endif