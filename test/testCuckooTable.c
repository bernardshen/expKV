#include "cuckoo_table.h"
#include "base_table.h"
#include "mm.h"
#include "testUtil.h"
#include <stdio.h>
#include <string.h>

int main() {
    BaseTable t;
    MemoryManager mm;
    int ret;

    testName("initMM");
    ret = initMM(&mm, CUCKOO);
    checkErr(ret, "initMM");

    testName("initTable");
    ret = initTable(&t, &mm, CUCKOO);
    checkErr(ret, "initTable");

    int64_t v = 16;
    int64_t retv;
    size_t retlen;
    char * k = "1111";
    ret = t.put(&t, k, 4, (char *)&v, sizeof(int64_t));
    checkErr(ret, "put");
    ret = t.get(&t, k, 4, (char *)&retv, &retlen);
    checkErr(ret, "get");
    printKV(k, 4, retv);

    v++;
    ret = t.put(&t, k, 4, (char *)&v, sizeof(int64_t));
    checkErr(ret, "put");
    ret = t.get(&t, k, 4, (char *)&retv, &retlen);
    checkErr(ret, "get");
    printKV(k, 4, retv);
    ret = t.del(&t, k, 4);
    checkErr(ret, "del");
    ret = t.get(&t, k, 4, (char *)&retv, &retlen);
    checkErr(ret, "get");
    printKV(k, 4, retv);

    for (int64_t i = 0; i < 100; i++) {
        char buf[16];
        sprintf(buf, "1111%ld", i);
        printf("put(%s, %ld)\n", buf, i);
        int keylen = strlen(buf);
        ret = t.put(&t, buf, keylen, (char *)&i, sizeof(int64_t));
        checkErr(ret, "put");
    }

    for (int64_t i = 0; i < 100; i ++) {
        char buf[16];
        sprintf(buf, "1111%ld", i);
        int keylen = strlen(buf);
        printf("get(%s)\n", buf);
        ret = t.get(&t, buf, keylen, (char *)&retv, &retlen);
        checkErr(ret, "get");
        printKV(buf, keylen, retv);
    }

    for (int64_t i = 10; i < 110; i++) {
        char buf[16];
        sprintf(buf, "1111%ld", i - 10);
        printf("put(%s, %ld)\n", buf, i);
        int keylen = strlen(buf);
        ret = t.put(&t, buf, keylen, (char *)&i, sizeof(int64_t));
        checkErr(ret, "put");
    }

    for (int64_t i = 0; i < 100; i ++) {
        char buf[16];
        sprintf(buf, "1111%ld", i);
        int keylen = strlen(buf);
        printf("get(%s)\n", buf);
        ret = t.get(&t, buf, keylen, (char *)&retv, &retlen);
        checkErr(ret, "get");
        printKV(buf, keylen, retv);
    }

    for (int64_t i = 0; i < 100; i ++) {
        char buf[16];
        sprintf(buf, "1111%ld", i);
        printf("del(%s)\n", buf);
        int keylen = strlen(buf);
        ret = t.del(&t, buf, keylen);
        checkErr(ret, "del");
    }

    for (int64_t i = 0; i < 100; i ++) {
        char buf[16];
        sprintf(buf, "1111%ld", i);
        int keylen = strlen(buf);
        printf("get(%s)\n", buf);
        ret = t.get(&t, buf, keylen, (char *)&retv, &retlen);
        if (ret == 0) {
            printKV(buf, keylen, retv);
        }
        checkErr(ret, "get");
        // printKV(buf, keylen, retv);
    }

    return 0;
}