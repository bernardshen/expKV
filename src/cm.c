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
#include <byteswap.h>
#include <errno.h>
#include "mm.h"
#include "utils.h"

#define KV_CQ_SIZE 10
#define KV_TCP_PORT "2333"
#define KV_TCP_BACKLOG 10
#define KV_LOCAL_MR_SIZE 1024
#define KV_RDMA_PROTNUM 1
#define KV_RDMA_GIDIDX 0

// ------ private functions ------
static int post_recv(PeerData * peer) {
	struct ibv_recv_wr rr;
	struct ibv_sge sge;
	struct ibv_recv_wr *bad_wr;
	int rc;
	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)peer->mr->addr;
	sge.length = KV_LOCAL_MR_SIZE;
	sge.lkey = peer->mr->lkey;
	/* prepare the receive work request */
	memset(&rr, 0, sizeof(rr));
	rr.next = NULL;
	rr.wr_id = peer->peerId;
	rr.sg_list = &sge;
	rr.num_sge = 1;
	/* post the Receive Request to the RQ */
	rc = ibv_post_recv(peer->qp, &rr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post RR\n");
    
	return rc; // return success here
}

// Called to SEND the buffer in the local memory to remote side
// The buffer must be prepared before calling
static int post_send(PeerData * peer) {
    struct ibv_send_wr sr;
    struct ibv_send_wr * badWr = NULL;
    struct ibv_sge sge;
    int rc;
    // prepare sge
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(peer->mr->addr);
    sge.length = KV_LOCAL_MR_SIZE;
    sge.lkey = peer->mr->lkey;
    // prepare sr
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = peer->peerId;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND;
    sr.send_flags = IBV_SEND_SIGNALED;
    // call post send
    rc = ibv_post_send(peer->qp, &sr, &badWr);
    if (rc) {
        printf("ibv_post_send send failed\n");
        return -1;
    }
    return 0; // return success here
}

static int post_read(PeerData * peer, uintptr_t addr, uint64_t len, uint32_t rkey) {
    struct ibv_send_wr sr;
    struct ibv_send_wr * badWr = NULL;
    struct ibv_sge sge;
    int rc;
    // prepare sge
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(peer->mr->addr);
    sge.length = len;
    sge.lkey = peer->mr->lkey;
    // prepare sr
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = peer->peerId;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = addr;
    sr.wr.rdma.rkey = rkey;
    // post send
    rc = ibv_post_send(peer->qp, &sr, &badWr);
    if (rc) {
        printf("ibv_post_send read failed\n");
        return -1;
    }
    return 0; // return success here
}

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


static int initCMServer(ConnectionManager * cm, char * _host) {
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


static int initCMClient(ConnectionManager * cm, char * _host) {
    int ret = -1;
    // check if host exists
    if (_host == NULL) {
        printf("host not provided\n");
        return -1;
    }

    // allocate common resources
    ret = initCMCommon(cm);
    if (ret < 0) {
        printf("initCMCommon failed\n");
        return ret;
    }

    // client socket initialization
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    char * hostName = _host;
    
    // set hints
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostName, KV_TCP_PORT, &hints, &servinfo) != 0) {
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
        ret = ibv_query_gid(cm->ctx, KV_RDMA_PROTNUM, KV_RDMA_GIDIDX, &myGid);
        if (ret) {
            printf("ibv_query_gid failed\n");
            printf("%s\n", strerror(errno));
            return -1;
        }
    }
    // prepare local data
    localConData.tableAddr = htonll((uintptr_t)(cm->tableMR->addr));
    localConData.tableRKey = htonl(cm->tableMR->rkey);
    localConData.itemPoolAddr = htonll((uintptr_t)(cm->itemPoolMr->addr));
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
    peer->peerId = cm->peerNum;

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

    // server post initial recv Request
    ret = post_recv(peer);
    if (ret < 0) {
        printf("server failed to post inital recv request\n");
        return -1;
    }

    ret = modify_qp_to_rts(qp);
    if (ret) {
        printf("modify_qp_to_rts failed\n");
        return -1;
    }

    // sync before client can send requests
    // use this sync to tell client their id
    int64_t nodeId = htonll(cm->peerNum);
    int64_t tmpId;
    if (sockSyncData(sockfd, sizeof(int64_t), &nodeId, &tmpId)) {
        printf("sock_sync_data 2 failed\n");
        return -1;
    }
    peer->peerId = cm->peerNum;

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
    remoteConData.itemPoolAddr = ntohll(tmpConData.itemPoolAddr);
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
    ret = modify_qp_to_rts(qp);
    if (ret) {
        printf("modify_qp_to_rts failed\n");
        return -1;
    }

    // sync before client can send requests
    // use this to get the nodeId from server
    uint64_t nodeId = htonll(cm->peerNum);
    uint64_t tmpId = 0;
    if (sockSyncData(cm->sock, sizeof(uint64_t), &nodeId, &tmpId)) {
        printf("sock_sync_data 2 failed\n");
        return -1;
    }
    nodeId = ntohll(tmpId);
    peer->remotePeerId = nodeId;
    printf("Client id: %ld\n", nodeId);
    peer->peerId = cm->peerNum;

    return 0; // return success here
}

// ------ public functions ------
int initCM(ConnectionManager * cm, char * host, NodeType nodeType) {
    int ret = -1;
    cm->nodeType = nodeType;
    cm->peerNum = 0;
    
    switch (nodeType) {
    case CLIENT:
        // client sock will be connected
        ret = initCMClient(cm, host);
        return ret;
    case SERVER:
        // server sock will be listening
        ret = initCMServer(cm, host);
        return ret;
    
    default:
        break;
    }
    return ret;
}

// called by the server to accept client requests
void CMServerConnect(ConnectionManager * cm) {
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
        if (peer == NULL) {
            printf("malloc peer failed\n");
            continue;
        }
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
    printf("tableAddr: 0x%08x, tableRkey: %d\n", peer->tableAddr, peer->tableRKey);
    printf("poolAddr: 0x%08x, poolRkey: %d\n", peer->itemPoolAddr, peer->itemPoolRKey);
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
    printf("tableAddr: 0x%08x, tableSize: %d, tableRkey: %d\n", mm->tableMR->addr, mm->tableMR->length, mm->tableMR->rkey);
    printf("poolAddr: 0x%08x, poolSize: %d, poolRkey: %d\n", mm->itemPoolMR->addr, mm->itemPoolMR->length, mm->itemPoolMR->rkey);
    return 0;
}


// ------ Wrapper Functions ------
// a wrapper for post recv
int CMPostRecv(ConnectionManager * cm, int64_t peerId) {
    PeerData * peer = cm->peers[peerId];
    int ret = -1;
    ret = post_recv(peer);
    if (ret < 0) {
        printf("post_receive failed\n");
        return -1;
    }
    return 0; // return success here
}

// a wrapper for post send
// need to serialize the message before sending
int CMPostSend(ConnectionManager * cm, int64_t nodeId, void * message, size_t mlen) {
    int ret = -1;
    PeerData * peer = cm->peers[nodeId];

    // prepare message
    memcpy(peer->mr->addr, message, mlen);
    
    // call post
    ret = post_send(peer);
    if (ret < 0) {
        printf("post send failed\n");
        return -1;
    }
    return 0; // return success here
}

// a wrapper for poll cq
int CMPollOnce(ConnectionManager * cm, __out int64_t * nodeId) {
    struct ibv_wc wc;
    int count = 0;
    *nodeId = -1;
    count = ibv_poll_cq(cm->cq, 1, &wc);
    if (count == 0) {
        return 0;
    } else if (count < 0) {
        printf("ibv_poll_cq failed\n");
        return -1;  // indicate poll cq failed
    }
    if (wc.status != IBV_WC_SUCCESS) {
        printf("wc failed status %s (%d) for wr_id %d\n", 
               ibv_wc_status_str(wc.status), wc.status, (int)wc.wr_id);
        return -2;  // indicate wc failed
    }
    
    // set nodeId for dispatcher
    *nodeId = -1; // -1 indicate self
    // only set nodeId when the wc is recv
    if (wc.opcode == IBV_WC_RECV) {
        *nodeId = wc.wr_id;
    } else if (wc.opcode == IBV_WC_RDMA_READ) {
        *nodeId = -2; // -2 indicate rdma read success;
    }
    return count;
}

// a wrapper for RDMA READ
int CMReadTable(ConnectionManager * cm, int64_t nodeId, void * addr, uint64_t len) {
    int ret = -1;
    PeerData * peer = cm->peers[nodeId];
    uint32_t rkey = peer->tableRKey;
    ret = post_read(peer, (uintptr_t)addr, len, rkey);
    if (ret < 0) {
        printf("post_read failed\n");
        return -1;
    }
    return 0; // return success here
}

int CMReadItemPool(ConnectionManager * cm, int64_t nodeId, void * addr, uint64_t len) {
    int ret = -1;
    PeerData * peer = cm->peers[nodeId];
    uint32_t rkey = peer->itemPoolRKey;
    ret = post_read(peer, (uintptr_t)addr, len, rkey);
    if (ret < 0) {
        printf("post_read failed\n");
        return -1;
    }
    return 0; //return success here
}