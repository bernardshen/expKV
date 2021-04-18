#include "RPCServer.h"
#include "testUtil.h"
#include <string.h>

int main(int argc, char ** argv) {
    RPCServer server;
    int ret;
    TableType t;

    if (argc < 2) {
        printf("Usage %s tableType\n", argv[0]);
        return 1;
    }
    if (!strcmp(argv[1], "cuckoo")) {
        t = CUCKOO;
    } else if (!strcmp(argv[1], "simple")) {
        t = SIMPLE;
    } else {
        t = SIMPLE;
    }

    testName("initRPCServer");
    ret = initRPCServer(&server, t);
    checkErr(ret, "initRPCServer");

    pthread_join(server.connectorThread, NULL);
}