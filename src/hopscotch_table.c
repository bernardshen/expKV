#include "hopscotch_table.h"
#include "base_table.h"
#include "mm.h"
#include "utils.h"
#include <stdint.h>
#include <assert.h>

// ==== private functions ====
// find the index of the key in the table, the table need to be locked
int hopscotchLookup(HopscotchTable * table, char * key, size_t klen) {
    uint64_t keyhash = hash((const uint8_t *)key, klen) % HOPSCOTCH_TABLE_SIZE;

    if (!table->table[keyhash].hopInfo) {
        return -1; // not found
    }
    
    // find all the neighbours
    for (int i = 0; i < HOPSCOTCH_TABLE_NEIGHBOUR; i++) {
        if (table->table[keyhash].hopInfo & (1 << i)) {
            HopscotchTableItem * item = &(table->table[keyhash + i]);
            if (HOPSCOTCH_TABLE_ITEM_VALID(item->itemVec)) {
                uint64_t tkeylen = HOPSCOTCH_TABLE_ITEM_KEYLEN(item->itemVec);
                if (compare_key(item->key, tkeylen, key, klen)) {
                    // found
                    return keyhash + i;
                }
            }
        }
    }
    return -1; // not found
}


// ==== public functions ====
int initHopscotchTable(BaseTable * t) {
    HopscotchTable * table = (HopscotchTable *)MMAllocTable(t->mm);

    if (table == NULL) {
        return -1;
    }

    // initialize data structure
    for (int i = 0; i < HOPSCOTCH_TABLE_SIZE; i++) {
        table->table[i].itemVec = 0;
        table->table[i].hopInfo = 0;
    }
    spin_unlock(&(table->lock));

    // bind HopscotchTable to BaseTable
    t->table = table;
    t->get = hopscotchTableGet;
    t->put = hopscotchTablePut;
    t->del = hopscotchTableDel;
    
    return 0;
}

int hopscotchTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen) {
    HopscotchTable * htable = (HopscotchTable *)(table->table);
    klen = min(klen, KV_KEYLEN_LIMIT);
    int ret = -1;

    uint64_t keyhash = hash((const uint8_t *)key, klen) % HOPSCOTCH_TABLE_SIZE;

    // find key in the table
    // lock the table
    spin_lock(&(htable->lock));
    int64_t id = hopscotchLookup(htable, key, klen);
    if (id >= 0) {
        // update the item inplace
        htable->table[id].value[0] = *(int64_t *)value;
        htable->table[id].value[1] = hash_crc((const uint8_t *)value, sizeof(int64_t));

        // unlock the table and return
        spin_unlock(&(htable->lock));
        return 0;
    }

    // find an empty slot
    int i = 0;
    int j = 0;
    int off = 0;
    for (i = keyhash; i < HOPSCOTCH_TABLE_SIZE; i++) {
        HopscotchTableItem * item = &(htable->table[i]);
        if (!HOPSCOTCH_TABLE_ITEM_VALID(item->itemVec)) {
            // empty item
            while (i - keyhash >= HOPSCOTCH_TABLE_NEIGHBOUR) {
                for (j = 1; j < HOPSCOTCH_TABLE_NEIGHBOUR; j++) {
                    if (htable->table[i - j].hopInfo) {
                        off = __builtin_ctz(htable->table[i - j].hopInfo);
                        if (off >= j) {
                            continue;
                        }
                        int tklen = HOPSCOTCH_TABLE_ITEM_KEYLEN(htable->table[i - j + off].itemVec);
                        memcpy(htable->table[i].key, htable->table[i - j + off].key, tklen);
                        memcpy(htable->table[i].value, htable->table[i - j + off].value, sizeof(int64_t) * 2);
                        memcpy(htable->table[i].itemVec, htable->table[i - j + off].itemVec, sizeof(uint8_t));
                        htable->table[i - j + off].itemVec = 0;
                        htable->table[i - j].hopInfo &= ~(1ULL << off);
                        htable->table[i - j].hopInfo |= (1ULL << j);
                        i = i - j + off;
                        break;
                    }
                }
            }
            if (j >= HOPSCOTCH_TABLE_NEIGHBOUR) {
                // unlock the table and return
                spin_unlock(&(htable->lock));
                return -1;
            }
        }

        off = i - keyhash;
        memcpy(htable->table[i].key, key, klen);
        htable->table[i].value[0] = *(int64_t *)value;
        htable->table[i].value[1] = hash_crc((const uint8_t *)value, sizeof(int64_t));
        htable->table[i].itemVec = HOPSCOTCH_TABLE_ITEM_VEC(1, klen);
        htable->table[keyhash].hopInfo |= (1ULL << off);
        spin_unlock(&(htable->lock));
        return 0;
    }

    spin_unlock(&(htable->lock));
    return -1;
}

int hopscotchTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen) {
    HopscotchTable * htable = (HopscotchTable *)(table->table);
    int ret = -1;
    klen = min(klen, KV_KEYLEN_LIMIT);

    // lock the table and lookup the item
    spin_lock(&(htable->lock));
    int id = hopscotchLookup(htable, key, klen);
    if (id >= 0) {
        HopscotchTableItem * item = &(htable->table[id]);
        *(int64_t *)value = item->value[0];
        *vlen = sizeof(int64_t);
        spin_unlock(&(htable->lock));
        return 0;
    }
    
    spin_unlock(&(htable->lock));
    return -1;
}

int hopscotchTableDel(BaseTable * table, char * key, size_t klen) {
    HopscotchTable * htable = (HopscotchTable *)(table->table);
    int ret = -1;
    klen = min(klen, KV_KEYLEN_LIMIT);
    uint64_t keyhash = hash((const uint8_t *)key, klen) % HOPSCOTCH_TABLE_SIZE;

    // lock the table and lookup the item
    spin_lock(&(htable->lock));
    int id = hopscotchLookup(htable, key, klen);
    if (id >= 0) {
        HopscotchTableItem * item = &(htable->table[id]);
        item->itemVec = 0;
        int off = id - keyhash;
        assert(off < HOPSCOTCH_TABLE_NEIGHBOUR);
        htable->table[keyhash].hopInfo &= ~(1ULL << off);
        
        spin_unlock(&(htable->lock));
        return 0;
    }

    spin_unlock(&(htable->lock));
    return -1;
}