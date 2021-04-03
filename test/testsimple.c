#include "base_table.h"

int main() {
    BaseTable t;
    int ret;
    ret = initTable(&t, SIMPLE);
    if (ret < 0) {
        printf("init error\n");
        return 1;
    }
    int64_t v = 16;
    int64_t * retv;
    size_t retlen;
    ret = t.put(&t, "1111", 4, v, sizeof(int64_t));
    if (ret < 0) {
        printf("put error\n");
    }
    ret = t.get(&t, "1111", 4, (char *)retv, retlen);
    if (ret < 0) {
        printf("get error\n");
    }
    printf("get key: %s value: %lld\n", "1111", *retv);
    ret = t.put(&t, "1111", 4, v+1, sizeof(int64_t));
    ret = t.get(&t, "1111", 4, (char *)retv, &retlen);
    printf("get key: %s value: %lld\n", "1111", *retv);
    return 0;
}