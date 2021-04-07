#include "RPCMessages.h"
#include "kvTypes.h"
#include "testUtil.h"
#include "cm.h"
#include "utils.h"
#include <stdint.h>

int main() {
    ConnectionManager cm;
    int ret = -1;

    testName("initCM");
    ret = initCM(&cm, CLIENT);
    checkErr(ret, "initCM");
    testEnd(ret);

    testName("CMClientConnect");
    ret = CMClientConnect(&cm);
    checkErr(ret, "CMClientConnect");
    testEnd(ret);

    testName("test put");
    RPCRequest message;
    message.reqType = htonl(PUT);
    strcpy(message.key, "1111");
    message.klen = htonll(4);
    message.value = htonll(4);
    message.vlen = htonll(sizeof(int64_t));
    message.nodeId = htonll(cm.peers[0]->peerId);
    // post recv first
    ret = CMPostRecv(&cm, 0);
    checkErr(ret, "CMPostRecv");
    // post send then
    ret = CMPostSend(&cm, 0, &message, sizeof(RPCRequest));
    checkErr(ret, "CMPostSend");
    while(1) {
        int64_t nodeId;
        int c;
        c = CMPollOnce(&cm, &nodeId);
        checkErr(c, "CMPollOnce");
        if (c > 0) {
            if (nodeId < 0) {
                continue;
            }
            RPCReply *reply = (RPCReply *)cm.peers[nodeId]->mr->addr;
            printf("success %d\n", ntohl(reply->success));
            break;
        }
    }
    testEnd(ret);

    testName("client get");
    message.reqType = htonl(GET);
    ret = CMPostRecv(&cm, 0);
    checkErr(ret, "CMPostRecv");
    ret = CMPostSend(&cm, 0, &message, sizeof(RPCRequest));
    checkErr(ret, "CMPostSend");
    while (1) {
        int64_t nodeId;
        int c;
        c = CMPollOnce(&cm, &nodeId);
        checkErr(c, "CMPollOnce");
        if (c > 0) {
            if (nodeId < 0) {
                continue;
            }
            RPCReply * reply = (RPCReply *)cm.peers[nodeId]->mr->addr;
            printf("success %d value %ld\n", ntohl(reply->success), ntohll(reply->value));
            break;
        }
    }
    testEnd(ret);

}