#include "RPCClient.h"
#include "utils.h"
#include "testUtil.h"
#include <assert.h>

int main(int argc, char ** argv) {
    TableType t;
    if (argc < 3) {
        printf("Usage: %s host tableType\n", argv[0]);
        return -1;
    }
    if (!strcmp(argv[2], "cuckoo")) {
        t = CUCKOO;
    } else if (!strcmp(argv[2], "simple")) {
        t = SIMPLE;
    } else if (!strcmp(argv[2], "hopscotch")) {
        t = HOPSCOTCH;
    } else if (!strcmp(argv[2], "block")) {
        t = BLOCK;
    } else {
        t = SIMPLE;
    }

    RPCClient client;
    int ret = -1;
    
    testName("initRPCClient");
    ret = initRPCClient(&client, argv[1], t);
    checkErr(ret, "initRPCClient");
    testEnd(ret);

    char key[16];
    int64_t value = 10;
    strcpy(key, "1111");

    testName("RPCClientKVPut");
    ret = RPCClientKVPut(&client, key, 4, (void *)&value, sizeof(int64_t));
    checkErr(ret, "RPCClientKVPut");
    testEnd(ret);

    // testName("RPCClientKVGet2S");
    int64_t retval;
    uint64_t vlen;
    // ret = RPCClientKVGet2S(&client, key, 4, &retval, &vlen);
    // checkErr(ret, "RPCClientKVGet2S");
    // printf("value: %ld, vlen: %lld\n", retval, vlen);
    // assert(retval == 10);
    // assert(vlen == sizeof(int64_t));
    // testEnd(ret);

    testName("RPCClientKVGet1S");
    ret = RPCClientKVGet1S(&client, key, 4, &retval, &vlen);
    checkErr(ret, "RPCClientKVGet1S");
    printf("value: %ld, vlen: %lld\n", retval, vlen);
    assert(retval == 10);
    assert(vlen == sizeof(int64_t));
    testEnd(ret);
}