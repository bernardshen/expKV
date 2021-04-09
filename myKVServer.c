#include "RPCServer.h"

int main() {
    RPCServer server;
    int ret;

    ret = initRPCServer(&server, SIMPLE);

    pthread_join(server.connectorThread, NULL);
}