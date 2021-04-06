#ifndef TESTUTIL_H
#define TESTUTIL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

void printKV(char * k, size_t klen, int64_t value) {
    printf("key: ");
    for (int i = 0; i < klen; i ++) {
        printf("%c", k[i]);
    }
    printf(" value: %ld\n", value);
}

void checkErr(int ret, char * str) {
    if (ret < 0) {
        printf("%s error\n", str);
        return;
    }
    printf("%s success\n", str);
}

void nl() {
    printf("\n");
}

void testName(char * str) {
    nl();
    printf("===== %s =====\n", str);
}

#endif