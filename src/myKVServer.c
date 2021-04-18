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
        printf("Table Type: cuckoo\n");
        t = CUCKOO;
    } else if (!strcmp(argv[1], "simple") || !strcmp(argv[1], "SIMPLE")) {
        printf("Table Type: simple\n");
        t = SIMPLE;
    } else if (!strcmp(argv[1], "hopscotch")) {
        printf("Table Type: hopscotch\n");
        t = HOPSCOTCH;
    } else {
        printf("Table Type: simple\n");
        t = SIMPLE;
    }

    ret = initRPCServer(&server, t);

    pthread_join(server.connectorThread, NULL);
}