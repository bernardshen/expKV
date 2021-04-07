#include "cm.h"
#include "mm.h"
#include "testUtil.h"
#include "utils.h"
#include <stdio.h>
#include <string.h>

int main() {
    ConnectionManager cm;
    int ret = 0;
    
    testName("initCM");
    ret = initCM(&cm, CLIENT);
    checkErr(ret, "initCM");

    testName("CMClientConnect");
    ret = CMClientConnect(&cm);
    checkErr(ret, "CMClientConnect");

    testName("CMPostSend");
    RPCRequest msg;
    msg.reqType = htonl(PUT);
    msg.value = htonll(10);
    memcpy(msg.key, "aaa", 3);
    ret = CMPostSend(&cm, 0, &msg, sizeof(RPCRequest));
    checkErr(ret, "CMPostSend");

    testName("CMPollOnce");
    while (1) {
        uint64_t nodeId;
        int c = CMPollOnce(&cm, &nodeId);
        checkErr(c, "CMPollOnce");
        if (c > 0) {
            break;
        }
    }
    return 0;
}