#include "RPCServer.h"
#include <string.h>

int main(int argc, char ** argv) {
    RPCServer server;
    int ret;
    TableType t;
    if (argc < 2) {
        printf("Usage %s tableType\n", argv[0]);
        return 1;
    }
    if (!strcmp(argv[1], "cuckoo") || !strcmp(argv[1], "CUCKOO")) {
        t = CUCKOO;
    } else if (!strcmp(argv[1], "simple") || !strcmp(argv[1], "SIMPLE")) {
        t = SIMPLE;
    } else {
        t = SIMPLE;
    }

    ret = initRPCServer(&server, t);

    pthread_join(server.connectorThread, NULL);
}