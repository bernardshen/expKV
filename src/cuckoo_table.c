#include "cuckoo_table.h"
#include "base_table.h"
#include "mm.h"
#include "utils.h"
#include <stdint.h>

uint64_t cuckoo_hash(int version, char * key, size_t klen) {
    uint64_t keyhash = hash((const uint8_t *)key, klen);
    switch (version) {
    case 0:
        return keyhash % CUCKOO_TABLE_SIZE;
    case 1:
        return (keyhash / CUCKOO_TABLE_SIZE) % CUCKOO_TABLE_SIZE;
    
    default:
        break;
    }
    return 0;
}

// ==== private functions ====
// be called recursively to place item in it
static int place(CuckooTable * table, int tableID, int cnt, int n, CuckooTableItem * insert_item) {
    int ret = -1;
    size_t pos[CUCKOO_TABLE_NUM];
    if (cnt == n) {
        printf("cuckoo place failed\n");
        return -1;
    }

    // get key and klen
    char * key = insert_item->key;
    size_t klen = CUCKOO_TABLE_ITEM_KEYLEN(insert_item->itemVec);
    // check if the two slot has stored the key
    for (int i = 0; i < CUCKOO_TABLE_NUM; i++) {
        // get hash
        pos[i] = cuckoo_hash(i, key, klen);

        // get item and lock the item
        CuckooTableItem * item = &(table->table[i][pos[i]]);
        spin_lock(&(item->lock));

        // check valid
        if (CUCKOO_TABLE_ITEM_VALID(item->itemVec)) {
            // check if the two key equals
            if (compare_key(item->key, CUCKOO_TABLE_ITEM_KEYLEN(item->itemVec), key, klen)) {
                // update the value
                memcpy(item->value, insert_item->value, sizeof(int64_t) * 2);
                // unlock the item and return success
                spin_unlock(&(item->lock));
                return 0;
            }
        }
        // release the lock
        spin_unlock(&(item->lock));
    }

    // get the item for the current slot
    CuckooTableItem * item = &(table->table[tableID][pos[tableID]]);
    spin_lock(&(item->lock));
    if (CUCKOO_TABLE_ITEM_VALID(item->itemVec)) {
        // reserve the current item
        CuckooTableItem * displace_item = (CuckooTableItem *)malloc(sizeof(CuckooTableItem));
        // replace the current item
        // cannot directly memcpy because the lock will be overwritten
        memcpy(item->key, insert_item->key, CUCKOO_TABLE_ITEM_KEYLEN(insert_item->itemVec));
        memcpy(item->value, insert_item->value, sizeof(int64_t) * 2);
        item->itemVec = insert_item->itemVec;

        // unlock the item and place the displace_item to a new place
        spin_unlock(&(item->lock));
        ret = place(table, (tableID + 1) % CUCKOO_TABLE_NUM, cnt + 1, n, displace_item);
        free(displace_item);
        return ret;
    } else {
        // the slot is free
        memcpy(item->key, insert_item->key, CUCKOO_TABLE_ITEM_KEYLEN(insert_item->itemVec));
        memcpy(item->value, insert_item->value, sizeof(int64_t) * 2);
        item->itemVec = insert_item->itemVec;
        
        // unlock the item and return success
        spin_unlock(&(item->lock));
        return 0;
    }
    // should never reach here
    return -1;
}



// ==== public functions ====
int initCuckooTable(BaseTable * t) {
    CuckooTable * table = (CuckooTable *)MMAllocTable(t->mm);

    if (table == NULL) {
        return -1;
    }

    // initialize data structure of cuckoo table
    for (int i = 0; i < CUCKOO_TABLE_NUM; i++) {
        for (int j = 0; j < CUCKOO_TABLE_SIZE; j++) {
            table->table[i][j].itemVec = 0;
            table->table[i][j].next = NULL;
            spin_unlock(&(table->table[i][j].lock));
        }
    }

    // bind CuckooTable to BaseTable
    t->table = table;
    t->get = cuckooTableGet;
    t->put = cuckooTablePut;
    t->del = cuckooTableDel;

    return 0;
}

int cuckooTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen) { 
    CuckooTable * ctable = (CuckooTable *)(table->table);
    int ret = -1;
    klen = min(klen, KV_KEYLEN_LIMIT);

    // construct an item
    CuckooTableItem * item = (CuckooTableItem *)malloc(sizeof(CuckooTableItem));
    memcpy(item->key, key, klen);
    item->value[0] = *(int64_t *)value;
    item->value[1] = hash_crc((const uint8_t *)value, sizeof(int64_t));
    item->itemVec = CUCKOO_TABLE_ITEM_VEC(1, klen);
    spin_unlock(&(item->lock));

    // place the item in the table
    ret = place(ctable, 0, 0, 20, item);
    free(item);
    if (ret < 0) {
        printf("palce failed\n");
        return -1;
    }

    // return success here
    return 0;
}

int cuckooTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen) {
    CuckooTable * ctable = (CuckooTable *)(table->table);
    int ret = -1;
    klen = min(klen, 16);

    // check all the tables
    for (int i = 0; i < CUCKOO_TABLE_NUM; i++) {
        int pos = cuckoo_hash(i, key, klen);
        CuckooTableItem * item = &(ctable->table[i][pos]);
        // lock the item
        spin_lock(&(item->lock));
        if (CUCKOO_TABLE_ITEM_VALID(item->itemVec)) {
            if (compare_key(item->key, CUCKOO_TABLE_ITEM_KEYLEN(item->itemVec), key, klen)) {
                int64_t tmp = item->value[0];
                while (item->value[1] != hash_crc((const uint8_t *)&tmp, sizeof(int64_t))) {
                    tmp = item->value[0];
                }
                *(int64_t *)value = tmp;
                *vlen = sizeof(int64_t);
                spin_unlock(&(item->lock));
                return 0;
            }
        }
        spin_unlock(&(item->lock));
    }

    // not found
    return -1;
}

int cuckooTableDel(BaseTable * table, char * key, size_t klen) {
    CuckooTable * ctable = (CuckooTable *)(table->table);
    int ret = -1;
    klen = min(klen, 16);

    // check all the tables
    for (int i = 0; i < CUCKOO_TABLE_NUM; i++) {
        int pos = cuckoo_hash(i, key, klen);
        CuckooTableItem * item = &(ctable->table[i][pos]);
        // lock the item
        spin_lock(&(item->lock));
        if (CUCKOO_TABLE_ITEM_VALID(item->itemVec)) {
            if (compare_key(item->key, CUCKOO_TABLE_ITEM_KEYLEN(item->itemVec), key, klen)) {
                item->itemVec = 0;
                spin_unlock(&(item->lock));
                return 0;
            }
        }
        spin_unlock(&(item->lock));
    }

    return -1;
}