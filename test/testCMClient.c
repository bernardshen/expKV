#include "cm.h"
#include "mm.h"
#include <stdio.h>

int main() {
    ConnectionManager cm;
    int ret = 0;
    printf("initing client cm\n");
    ret = initCM(&cm, CLIENT);
    if (ret < 0) {
        printf("initCM failed\n");
    }

    ret = CMClientConnect(&cm);
    if (ret < 0) {
        printf("CMClientConnect failed\n");
    }

    printf("success\n");
    return 0;
}