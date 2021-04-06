#include "cm.h"
#include "mm.h"
#include "testUtil.h"
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
    RPCMessage msg;
    msg.reqType = PUT;
    msg.value = 10;
    memcpy(msg.key, "aaa", 3);
    ret = CMPostSend(&cm, 0, &msg);
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