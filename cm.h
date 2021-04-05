#ifndef CM_H
#define CM_H

#include <infiniband/verbs.h>
#include <stdint.h>
#include "kvTypes.h"
#include "spinlock.h"
#include "mm.h"

#define KV_CONN_PORT 2333
#define KV_PEER_NUM 512

typedef struct _PeerData {
    // data on both server and client side
    struct ibv_qp * qp; // the qp for communication
    struct ibv_mr * mr; // the mr for send/recv only local access

    // data on client side
    uint64_t tableAddr;
    uint64_t itemPoolAddr;
    uint32_t tableRKey;
    uint32_t itemPoolRKey;
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

    // peer information **Append-only**
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





#endif