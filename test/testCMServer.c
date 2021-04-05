#include "cm.h"
#include "mm.h"
#include <stdio.h>

int main() {
    MemoryManager mm;
    ConnectionManager cm;
    int ret = -1;

    ret = initMM(&mm, SIMPLE);
    if (ret < 0) {
        printf("initMM failed\n");
        return -1;
    }

    ret = initCM(&cm, SERVER);
    if (ret < 0) {
        printf("initCM failed\n");
        return -1;
    }

    printf("registering mr\n");
    ret = CMServerRegisterMR(&cm, &mm);
    if (ret < 0) {
        printf("CMServerRegisterMR failed\n");
        return -1;
    }

    printf("Serving clients\n");
    CMServerConnect(&cm);
    if (ret < 0) {
        printf("CMServerConnect failed\n");
        return -1;
    }
    return 0;
}