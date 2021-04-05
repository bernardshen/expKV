#include <stdio.h>
#include <stdint.h>
#include "cm.h"
#include "kvTypes.h"
#include <infiniband/verbs.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include "mm.h"

#define KV_CQ_SIZE 10
#define KV_TCP_PORT 2333
#define KV_TCP_BACKLOG 10
#define KV_LOCAL_MR_SIZE 512
#define KV_RDMA_PROTNUM 1
#define KV_RDMA_GIDIDX 0

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

// ------ private functions ------
// create some common datastructures (ibv_ctx, ibv_pd, ibv_cq)
static int initCMCommon(ConnectionManager * cm) {
    struct ibv_device **dev_list = NULL;
    struct ibv_device * dev = NULL;
    struct ibv_context * ctx = NULL;
    struct ibv_pd * pd = NULL;
    struct ibv_cq * cq = NULL;
    int numDevices = 0;

    // get device list
    dev_list = ibv_get_device_list(&numDevices);
    if (!dev_list || !numDevices) {
        printf("ibv_get_device_list failed\n");
        return -1;
    }
    
    // get device
    dev = dev_list[0];
    if (!dev) {
        printf("Device not found\n");
        goto free_and_exit;
    }

    // open device
    ctx = ibv_open_device(dev);
    if (!ctx) {
        printf("ibv_open_device failed\n");
        goto free_and_exit;
    }

    // free device list
    ibv_free_device_list(dev_list);

    // query port
    if (ibv_query_port(ctx, 1, &cm->portAttr)) {
        printf("ibv_query_port failed\n");
        goto free_and_exit;
    }

    // allocate pd
    pd = ibv_alloc_pd(ctx);
    if (!pd) {
        printf("ibv_alloc_pd failed\n");
        goto free_and_exit;
    }

    // alloc cq
    cq = ibv_create_cq(ctx, KV_CQ_SIZE, NULL, NULL, 0);
    if (!cq) {
        printf("ibv_create_cq failed\n");
        goto free_and_exit;
    }

    // assign all the resources to the cm
    cm->ctx = ctx;
    cm->pd = pd;
    cm->cq = cq;

    return 0; // return here if success

free_and_exit:
    if (dev_list) {
        ibv_free_device_list(dev_list);
    }
    if (ctx) {
        ibv_close_device(ctx);
    }
    if (pd) {
        ibv_dealloc_pd(pd);
    }
    if (cq) {
        ibv_destroy_cq(cq);
    }
    return -1; // return here if failed
}


static int initCMServer(ConnectionManager * cm) {
    int ret = -1;
    // allocate common resources
    ret = initCMCommon(cm);
    if (ret < 0) {
        printf("initCMCommon failed\n");
        return ret;
    }

    // server socket initialization
    int sockfd = -1;
    struct addrinfo hints, *servinfo, *p;
    int on = 1;

    // set hints
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    // get address info
    if (getaddrinfo(NULL, KV_TCP_PORT, &hints, &servinfo) != 0) {
        printf("getaddrinfo failed\n");
        return -1;
    }

    // bind the socket
    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            printf("socket failed\n");
            continue;
        }
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(int)) == -1) {
            printf("setsockopt failed\n");
            continue;
        }
        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            printf("bind failed\n");
            continue;
        }
        break;
    }

    freeaddrinfo(servinfo);
    if (sockfd == -1) {
        printf("failed to bind\n");
        return -1;
    }

    if (listen(sockfd, KV_TCP_BACKLOG) == -1) {
        printf("failed to listen\n");
        close(sockfd);
        return -1;
    }

    // assign the sockfd to the cm
    cm->sock = sockfd;

    return 0; // return success here
}


static int initCMClient(ConnectionManager * cm) {
    int ret = -1;
    // allocate common resources
    ret = initCMCommon(cm);
    if (ret < 0) {
        printf("initCMCommon failed\n");
        return ret;
    }

    // client socket initialization
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    
    // set hints
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo("localhost", KV_TCP_PORT, &hints, &servinfo) != 0) {
        printf("getaddrinfo failed\n");
        return -1;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            printf("socket failed\n");
            continue;
        }
        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            printf("connect failed\n");
            continue;
        }
        break;
    }

    freeaddrinfo(servinfo);
    if (sockfd == -1) {
        printf("cannot create socket\n");
        return -1;
    }

    // assign the sockfd to cm
    cm->sock = sockfd;
    return 0; // return success here
}

static int sockSyncData(int sock, int xferSize, char *localData, char *remoteData) {
    int rc;
    int readBytes = 0;
    int totalReadBytes = 0;
    rc = write(sock, localData, xferSize);
    if (rc < xferSize) {
        printf("write data failed\n");
    } else 
        rc = 0;
    while (!rc && totalReadBytes < xferSize) {
        readBytes = read(sock, remoteData, xferSize);
        if (readBytes > 0) 
            totalReadBytes += readBytes;
        else
            rc = readBytes;
    }
    return rc;
}

static struct ibv_qp * CMCreateQP(ConnectionManager * cm) {
    struct ibv_qp_init_attr initAttr;
    struct ibv_qp * qp = NULL;
    memset(&initAttr, 0, sizeof(struct ibv_qp_init_attr));
    initAttr.qp_type = IBV_QPT_RC;
    initAttr.sq_sig_all = 1;
    initAttr.send_cq = cm->cq;
    initAttr.recv_cq = cm->cq;
    initAttr.cap.max_send_wr = 1;
    initAttr.cap.max_recv_wr = 1;
    initAttr.cap.max_send_sge = 1;
    initAttr.cap.max_recv_sge = 1;
    
    qp = ibv_create_qp(cm->pd, &initAttr);
    return qp;
}

int modify_qp_to_init(struct ibv_qp *qp) {
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = 1;
	attr.pkey_index = 0;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to INIT\n");
	return rc;
}

int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid) {
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_256;
	attr.dest_qp_num = remote_qpn;
	attr.rq_psn = 0;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 0x12;
	attr.ah_attr.is_global = 0;
	attr.ah_attr.dlid = dlid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = 1;
	{
		attr.ah_attr.is_global = 1;
		attr.ah_attr.port_num = 1;
		memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.sgid_index = 0;
		attr.ah_attr.grh.traffic_class = 0;
	}
	flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTR\n");
	return rc;
}

int modify_qp_to_rts(struct ibv_qp *qp) {
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 0x12;
	attr.retry_cnt = 6;
	attr.rnr_retry = 0;
	attr.sq_psn = 0;
	attr.max_rd_atomic = 1;
	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTS\n");
	return rc;
}

// called by server to connect qp with the client
static int serverConnectQP(ConnectionManager * cm, PeerData * peer, int sockfd) {
    struct ibv_qp * qp;
    int ret = -1;
    qp = CMCreateQP(cm);
    if (qp == NULL) {
        printf("create qp failed\n");
        return -1;
    }
    
    // prepare connection data
    ConData localConData;
    ConData remoteConData;
    ConData tmpConData;
    union ibv_gid myGid;
    
    // prepare gid
    {
        ret = ibv_query_gid(cm->ctx, KV_RDMA_GIDIDX, KV_RDMA_GIDIDX, &myGid);
        if (ret) {
            printf("ibv_query_gid failed\n");
            return -1;
        }
    }
    // prepare local data
    localConData.tableAddr = htonll((uintptr_t)cm->tableMR->addr);
    localConData.tableRKey = htonl(cm->tableMR->rkey);
    localConData.itemPoolRKey = htonll((uintptr_t)cm->itemPoolMr->addr);
    localConData.itemPoolRKey = htonl(cm->itemPoolMr->rkey);
    localConData.qpNum = htonl(qp->qp_num);
    localConData.lid = htons(cm->portAttr.lid);
    memcpy(localConData.gid, &myGid, 16);

    // exchange connection data
    if (sockSyncData(sockfd, sizeof(ConData), (char *)&localConData, (char *)&tmpConData) < 0) {
        printf("sock_sync_data 0 failed\n");
        return -1;
    }
    remoteConData.tableAddr = 0;
    remoteConData.tableRKey = 0;
    remoteConData.itemPoolAddr = 0;
    remoteConData.itemPoolRKey = 0;
    remoteConData.qpNum = ntohl(tmpConData.qpNum);
    remoteConData.lid = ntohs(tmpConData.lid);
    memcpy(remoteConData.gid, tmpConData.gid, 16);

    // create mr for local access
    int access = IBV_ACCESS_LOCAL_WRITE;
    void * lbuf = malloc(KV_LOCAL_MR_SIZE);
    struct ibv_mr * lmr = ibv_reg_mr(cm->pd, lbuf, KV_LOCAL_MR_SIZE, access);
    if (!lmr) {
        printf("Failed to register local mr\n");
        return -1;
    }

    // save data to peer
    peer->itemPoolAddr = NULL;
    peer->itemPoolRKey = NULL;
    peer->tableAddr = NULL;
    peer->tableRKey = NULL;
    peer->qp = qp;
    peer->mr = lmr;

    // connect qp
    ret = modify_qp_to_init(qp);
    if (ret) {
        printf("modify_qp_to_init failed\n");
        return -1;
    }
    ret = modify_qp_to_rtr(qp, remoteConData.qpNum, remoteConData.lid, remoteConData.gid);
    if (ret) {
        printf("modify_qp_to_rtr failed\n");
        return -1;
    }
    ret = post_receive(peer);
    if (ret) {
        printf("Failed to post initial rr\n");
        return -1;
    }
    ret = modify_qp_to_rts(qp);
    if (ret) {
        printf("modify_qp_to_rts failed\n");
        return -1;
    }

    // sync before client can send requests
    char tmpChar;
    if (sockSyncData(sockfd, 1, "R", &tmpChar)) {
        printf("sock_sync_data 2 failed\n");
        return -1;
    }

    return 0; // return success here
}

// called by client to connect qp with the server
static int clientConnectQP(ConnectionManager * cm, PeerData * peer) {
    struct ibv_qp * qp;
    int ret = -1;
    
    // create qp
    qp = CMCreateQP(cm);
    if (qp == NULL) {
        printf("CMCreateQP failed\n");
        return -1;
    }

    // prepare connection data
    ConData localConData;
    ConData remoteConData;
    ConData tmpConData;
    union ibv_gid myGid;

    // prepare gid
    {
        ret = ibv_query_gid(cm->ctx, KV_RDMA_PROTNUM, KV_RDMA_GIDIDX, &myGid);
        if (ret) {
            printf("ibv_query_gid failed\n");
            return -1;
        }
    }
    // prepare local data
    localConData.tableAddr = htonll((uintptr_t)NULL);
    localConData.tableRKey = htonl(1);
    localConData.itemPoolAddr = htonll((uintptr_t)NULL);
    localConData.itemPoolRKey = htonl(1);
    localConData.qpNum = htonl(qp->qp_num);
    localConData.lid = htons(cm->portAttr.lid);
    memcpy(localConData.gid, &myGid, 16);

    // exchange connection data
    if (sockSyncData(cm->sock, sizeof(ConData), (char *)&localConData, (char *)&tmpConData) < 0) {
        printf("sock_sync_data 0 failed\n");
        return -1;
    }
    remoteConData.tableAddr = ntohll(tmpConData.tableAddr);
    remoteConData.tableRKey = ntohl(tmpConData.tableRKey);
    remoteConData.itemPoolRKey = ntohll(tmpConData.itemPoolAddr);
    remoteConData.itemPoolRKey = ntohl(tmpConData.itemPoolRKey);
    remoteConData.qpNum = ntohl(tmpConData.qpNum);
    remoteConData.lid = ntohs(tmpConData.lid);
    memcpy(remoteConData.gid, tmpConData.gid, 16);

    // create mr for local access
    int access = IBV_ACCESS_LOCAL_WRITE;
    void * lbuf = malloc(KV_LOCAL_MR_SIZE);
    struct ibv_mr * lmr = ibv_reg_mr(cm->pd, lbuf, KV_LOCAL_MR_SIZE, access);
    if (!lmr) {
        printf("Failed to register local mr\n");
        return -1;
    }

    // save data to peer
    peer->tableAddr = remoteConData.tableAddr;
    peer->tableRKey = remoteConData.tableRKey;
    peer->itemPoolAddr = remoteConData.itemPoolAddr;
    peer->itemPoolRKey = remoteConData.itemPoolRKey;
    peer->qp = qp;
    peer->mr = lmr;

    // connect qp
    ret = modify_qp_to_init(qp);
    if (ret) {
        printf("modify_qp_to_init failed\n");
        return -1;
    }
    ret = modify_qp_to_rtr(qp, remoteConData.qpNum, remoteConData.lid, remoteConData.gid);
    if (ret) {
        printf("modify_qp_to_rtr failed\n");
        return -1;
    }
    ret = post_receive(peer);
    if (ret) {
        printf("Failed to post initial rr\n");
        return -1;
    }
    ret = modify_qp_to_rts(qp);
    if (ret) {
        printf("modify_qp_to_rts failed\n");
        return -1;
    }

    // sync before client can send requests
    char tmpChar;
    if (sockSyncData(cm->sock, 1, "R", &tmpChar)) {
        printf("sock_sync_data 2 failed\n");
        return -1;
    }

    return 0; // return success here
}

// ------ public functions ------
int initCM(ConnectionManager * cm, NodeType nodeType) {
    int ret = -1;
    cm->nodeType = nodeType;
    
    switch (nodeType) {
    case CLIENT:
        // client sock will be connected
        ret = initCMClient(cm);
        return ret;
    case SERVER:
        // server sock will be listening
        ret = initCMServer(cm);
        return ret;
    
    default:
        break;
    }
    return ret;
}

// called by the server to accept client requests
void CMServerConnect(ConnectionManager * cm) {
    socklen_t sin_size;
    struct sockaddr_storage their_addr;
    socklen_t sin_size = sizeof(their_addr);
    int new_fd;

    // loop to server client requests
    while (1) {
        int succ = 0;
        new_fd = accept(cm->sock, (struct sockaddr *)&their_addr, &sin_size);
        printf("client connected\n");
        // allocate new peer structure
        PeerData *peer = (PeerData *)malloc(sizeof(PeerData));
        succ = serverConnectQP(cm, peer, new_fd);
        if (succ < 0) {
            printf("client qp connect failed\n");
            continue;
        }

        // add peer to cm
        cm->peers[cm->peerNum] = peer;
        cm->peerNum ++;

        printf("client qp connect success\n");
    }

}

// called by clients to connect to the server
int CMClientConnect(ConnectionManager * cm) {
    int succ = 0;
    PeerData * peer = (PeerData *)malloc(sizeof(PeerData));
    succ = clientConnectQP(cm, peer);
    if (succ < 0) {
        printf("client failed to connect qp\n");
        return -1;
    }
    
    // save peer to cm
    cm->peers[cm->peerNum] = peer;
    cm->peerNum ++;
    return 0;
}

int CMServerRegisterMR(ConnectionManager * cm, MemoryManager * mm) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    int ret = -1;
    ret = MMRegisterMR(mm, cm->pd, access);
    if (ret < 0) {
        printf("register mr failed\n");
        return ret;
    }

    // assign the two mr to the cm for connection management
    cm->tableMR = mm->tableMR;
    cm->itemPoolMr = mm->itemPoolMR;
    return 0;
}