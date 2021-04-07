#include "RPCServer.h"
#include "testUtil.h"

int main() {
    RPCServer server;
    int ret;

    testName("initRPCServer");
    ret = initRPCServer(&server, SIMPLE);
    checkErr(ret, "initRPCServer");

    pthread_join(server.connectorThread, NULL);
}