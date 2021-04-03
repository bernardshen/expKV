#include <string.h>
#include <stdint.h>
#include "simple_table.h"
#include "base_table.h"
#include "utils.h"

int initSimpleTable(BaseTable * t) {
    SimpleTable * table = (SimpleTable *)malloc(sizeof(SimpleTable));

    if (table == NULL) {
        return -1;
    }
    
    // initialize the data structure of SimpleTable
    for (int i = 0; i < SIMPLE_TABLE_SIZE; i++) {
        table->table[i].itemVec = 0;
        table->table[i].next = NULL;
    }

    // bind SimpleTable to BaseTable
    t->table = table;
    t->get = simpleTableGet;
    t->put = simpleTablePut;
    t->del = simpleTableDel;
    
    return 0;
}

int simpleTablePut(void * table, char * key, size_t klen, char * value, size_t vlen) {
    SimpleTable *stable = (SimpleTable *)table;
    klen = min(klen, 16);
    uint64_t keyhash = hash(key, klen) % SIMPLE_TABLE_SIZE;

    // get value
    // TODO lock the item
    SimpleTableItem * item = &(stable->table[(size_t)keyhash]);
    if (SIMPLE_TABLE_ITEM_VALID(item->itemVec)) {
        // find potential keys
        SimpleTableItem * p;
        for (p = item; p; p = p->next) {
            size_t pklen = SIMPLE_TABLE_ITEM_KEYLEN(item->itemVec);
            if (compare_key(p->key, pklen, key, klen)) { 
                // update the item
                p->value[0] = *(int64_t *)value;
                p->value[1] = hash_md5((const uint8_t *)&p->value[0], sizeof(uint64_t));
                break;
            }
        }
        if (p == NULL) {
            // inster the item
            p = (SimpleTableItem*)malloc(sizeof(SimpleTableItem));
            if (p == NULL) {
                return -1;
            }

            // setup the item
            memcpy(p->key, key, klen);
            p->value[0] = *(int64_t *)value;
            p->value[1] = hash_md5((const uint8_t *)&p->value[0], sizeof(int64_t));
            p->itemVec = SIMPLE_TABLE_ITEM_VEC(1, klen); // update itemVec

            // add the item to the linked list
            p->next = item->next;
            item->next = p;
        }
    } else {
        memcpy(item->key, key, klen);
        item->value[0] = *(int64_t *)value;
        item->value[1] = hash_md5((const uint8_t *)&(item->value[0]), sizeof(int64_t));
        item->itemVec = SIMPLE_TABLE_ITEM_VEC(1, klen);
    }
    // TODO: unlock the item
    return 0;
}

int simpleTableGet(void * table, char * key, size_t klen, char * value, size_t * vlen) {
    SimpleTable *stable = (SimpleTable *)table;
    klen = min(klen, 16);
    uint64_t keyhash = hash(key, klen) % SIMPLE_TABLE_SIZE;
    
    // TODO: lock the item
    SimpleTableItem *item = &(stable->table[keyhash]);
    if (SIMPLE_TABLE_ITEM_VALID(item->itemVec)) {
        SimpleTableItem * p;
        for (p = item; p; p = p->next) {
            size_t keylen = SIMPLE_TABLE_ITEM_KEYLEN(p->itemVec);
            if (compare_key(p->key, keylen, key, klen)) {
                int64_t tmp = p->value[0];
                while(p->value[1] != hash_md5((const uint8_t *)&tmp, sizeof(int64_t))) {
                    tmp = p->value[0];
                }
                *(int64_t *)value = tmp;
                *vlen = sizeof(int64_t);
                
                // TODO: unlock the item
                return 0;
            }
        }
    }

    // not found
    // TODO: unlock the item
    return -1;
}

int simpleTableDel(void * table, char * key, size_t klen) {
    SimpleTable *stable = (SimpleTable *)table;
    klen = min(klen, 16);
    uint64_t keyhash = hash(key, klen) % SIMPLE_TABLE_SIZE;

    // TODO: lock the item
    SimpleTableItem *item = &(stable->table[keyhash]);
    if (SIMPLE_TABLE_ITEM_VALID(item->itemVec)) {
        SimpleTableItem * p;
        SimpleTableItem * prev;
        for (prev = p = item; p; prev = p, p = p->next) {
            size_t keylen = SIMPLE_TABLE_ITEM_KEYLEN(p->itemVec);
            if (compare_key(p->key, keylen, key, klen)) {
                if (p == item) {
                    if (p->next == NULL) {
                        p->itemVec = 0;
                    } // p == &item and p.next == NULL
                    else {
                        SimpleTableItem * tmp = p->next;
                        size_t len2 = SIMPLE_TABLE_ITEM_KEYLEN(tmp->itemVec);
                        memcpy(item->key, tmp->key, len2);
                        memcpy(item->value, tmp->value, sizeof(int64_t) * 2);
                        item->itemVec = tmp->itemVec;
                        item->next = tmp->next;
                        free(tmp);
                    }
                } else {
                    prev->next = p->next;
                    free(p);
                }
                // TODO: unlock item
                return 0;
            }
        }
    }
    // TODO: unlock the item
    return -1;
}