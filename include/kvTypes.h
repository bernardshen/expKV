#ifndef KV_TYPES
#define KV_TYPES

#define KV_KEYLEN_LIMIT 16
#define __out

typedef enum _TableType {
    SIMPLE,
    BLOCK,
    CUCKOO,
    HOPSCOTCH,
} TableType;

typedef enum _ReqType {
    PUT,
    DEL,
    GET,
} ReqType;

typedef enum _NodeType {
    CLIENT,
    SERVER,
} NodeType;

#endif