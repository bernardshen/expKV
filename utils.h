#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/md5.h>
#include <byteswap.h>
#include "city.h"

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

static uint64_t hash(const uint8_t * key, size_t len) {
    return CityHash64((const char *)key, len);
}

static uint64_t hash_md5(const uint8_t *key, size_t len) {
    size_t temp_hash[(MD5_DIGEST_LENGTH + sizeof(size_t) - 1) / sizeof(size_t)];
    MD5(key, len, (uint8_t *)temp_hash);
    // assert(8 <= MD5_DIGEST_LENGTH);
    return *(size_t *)temp_hash;
}

static int compare_key(char * key1, size_t len1, char * key2, size_t len2) {
    for (int i = 0; i < len1 && i < len2; i++) {
        if (key1[i] - key2[i])
            return 0;
    }
    return len1 == len2;
}

static size_t min(size_t len1, size_t len2) {
    if (len1 < len2) 
        return len1;
    return len2;
}

#endif