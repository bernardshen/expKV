#ifndef KV_TYPES
#define KV_TYPES

typedef enum _TableType {
    SIMPLE,
    COCKOO,
    HOPSCOTCH,
} TableType;

typedef enum _ReqType {
    PUT,
    DEL,
} ReqType;

typedef enum _NodeType {
    CLIENT,
    SERVER,
} NodeType;

#endif