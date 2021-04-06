#include "cm.h"
#include "mm.h"
#include "testUtil.h"
#include <stdio.h>
#include <pthread.h>
#include <byteswap.h>

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

void testth1(void * cm) {
    testName("CMServerConnect");
    CMServerConnect((ConnectionManager *)cm);
}

int main() {
    MemoryManager mm;
    ConnectionManager cm;
    int ret = -1;

    testName("initMM");
    ret = initMM(&mm, SIMPLE);
    checkErr(ret, "initMM");

    testName("initCM");
    ret = initCM(&cm, SERVER);
    checkErr(ret, "initCM");

    testName("CMServerRegisterMR");
    ret = CMServerRegisterMR(&cm, &mm);
    checkErr(ret, "CMServerRegisterMR");

    testName("create server listen thread\n");
    pthread_t ptid;
    ret = pthread_create(&ptid, NULL, testth1, (void *)&cm);
    checkErr(ret, "create thread");

    testName("CMPollOnce");
    uint64_t nid = -1;
    while (1) {
        int c = CMPollOnce(&cm, &nid);
        checkErr(c, "CMPollOnce");
        if (c > 0) {
            break;
        }
    }
    RPCMessage *c = cm.peers[0]->mr->addr;
    c->reqType = ntohl(c->reqType);
    printf("key: %c%c%c\n", c->key[0], c->key[1], c->key[2]);
    printf("value: %ld\n", ntohll(c->value));
    printf("wrid: %ld\n", nid);

    pthread_join(ptid, NULL);
    return 0;
}