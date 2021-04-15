#include "block_table.h"
#include <string.h>
#include <stdint.h>
#include "base_table.h"
#include "utils.h"
#include "spinlock.h"
#include "mm.h"
#include <assert.h>

// ==== private functions ====

// find the item in the block
// if found return its index, else reutrn -1
// the empty id returns the first empty slot
static int findKVInBlock(BlockTableItem * block, char * key, size_t klen, __out int * _emptyId) {
    if (block == NULL) {
        *_emptyId = -1;
        return -1;
    }
    if (block->emptyCnt == BLOCK_TABLE_BLOCK_SIZE) {
        *_emptyId = 0;
        return -1;
    }

    int emptyId = -1;
    // iterate all the items
    for (int i = 0; i < BLOCK_TABLE_BLOCK_SIZE; i ++) {
        uint8_t valid = block->itemVec[i];
        // if the item is valid
        if (BLOCK_TABLE_ITEM_VALID(valid)) {
            char * target_key = block->key[i];
            size_t target_klen = BLOCK_TABLE_ITEM_KEYLEN(valid);
            if (compare_key(target_key, target_klen, key, klen)) {
                return i;
            }
        } else if (emptyId < 0) {
            emptyId = i;
        }
    }
    
    // set emptyid
    *_emptyId = emptyId;
    return -1;
}

static void putKVInBlock(BlockTableItem * block, char * key, size_t klen, char * value, size_t vlen, size_t index) {
    // put key
    memcpy(block->key[index], key, klen);
    // put value
    block->value[index][0] = *(int64_t *)value;
    // block->value[index][1] = hash_md5((const uint8_t *)&block->value[index][0], sizeof(int64_t));
    block->value[index][1] = hash_crc((const uint8_t *)&block->value[index][0], sizeof(int64_t));
    // setup item vector
    block->itemVec[index] = BLOCK_TABLE_ITEM_VEC(1, klen);
}


// ==== public functions ====

int initBlockTable(BaseTable * t) {
    BlockTable * table = (BlockTable *)MMAllocTable(t->mm);

    if (table == NULL) {
        return -1;
    }

    for (int i = 0; i < BLOCK_TABLE_SIZE; i++) {
        memset(table->table[i].itemVec, 0, sizeof(uint8_t) * BLOCK_TABLE_BLOCK_SIZE);
        table->table[i].next = NULL;
        table->table[i].emptyCnt = BLOCK_TABLE_BLOCK_SIZE;
        spin_unlock(&(table->table[i].lock));
    }

    t->table = table;
    t->get = blockTableGet;
    t->put = blockTablePut;
    t->del = blockTableDel;

    return 0;
}

int blockTablePut(BaseTable * table, char * key, size_t klen, char * value, size_t vlen) {
    BlockTable * btable = (BlockTable *)table->table;
    klen = min(klen, 16);
    uint64_t keyhash = hash(key, klen) % BLOCK_TABLE_SIZE;
    int ret = -1;

    // get value & lock the item
    BlockTableItem * item = &(btable->table[(size_t)keyhash]);
    spin_lock(&(item->lock));

    // iterate through chain of blocks to find the slot to insert
    BlockTableItem * p = NULL;
    BlockTableItem * firstEmptyBlock = NULL;
    int firstEmpty = -1;
    // iterate blocks
    for (p = item; p; p = p->next) {
        int emptyId = -1;
        int found = -1;
        found = findKVInBlock(p, key, klen, &emptyId);
        if (found >= 0) {
            putKVInBlock(p, key, klen, value, vlen, found);
            spin_unlock(&(item->lock));
            return 0;
        }
        if (firstEmpty == -1) {
            firstEmpty = emptyId;
            firstEmptyBlock = p;
        }
    }

    // if there exists empty slot
    if (firstEmpty >= 0) {
        putKVInBlock(firstEmptyBlock, key, klen, value, vlen, firstEmpty);
        firstEmptyBlock->emptyCnt -= 1;
        spin_unlock(&(item->lock));
        return 0;
    }

    // not found and all blocks are full -> allocate new block
    p = (BlockTableItem *)MMAllocItem(table->mm);
    if (p == NULL) {
        printf("MMAllocItem failed\n");
        spin_unlock(&(item->lock));
        return -1;
    }

    // setup the item
    memset(p->itemVec, 0, sizeof(uint8_t) * BLOCK_TABLE_BLOCK_SIZE);
    p->next = NULL;
    // put the item
    putKVInBlock(p, key, klen, value, vlen, 0);
    p->emptyCnt = BLOCK_TABLE_BLOCK_SIZE - 1;

    // link the block to the table
    p->next = item->next;
    item->next = p;
    
    // unlock the item
    spin_unlock(&(item->lock));
    return 0;
}

int blockTableGet(BaseTable * table, char * key, size_t klen, char * value, size_t *vlen) {
    BlockTable * btable = (BlockTable *)table->table;
    klen = min(klen, 16);
    uint64_t keyhash = hash(key, klen) % BLOCK_TABLE_SIZE;

    // get the item and lock the item
    BlockTableItem * item = &(btable->table[keyhash]);
    spin_lock(&(item->lock));

    // iterate through all the blocks to find the item
    BlockTableItem * p = NULL;
    for (p = item; p; p = p->next) {
        int found = -1;
        int emptyId = -1;
        found = findKVInBlock(p, key, klen, &emptyId);
        if (found >= 0) {
            int64_t tmp = p->value[found][0];
            // while (p->value[found][1] != hash_md5((const uint8_t *)&tmp, sizeof(int64_t))) {
            while (p->value[found][1] != hash_crc((const uint8_t *)&tmp, sizeof(int64_t))) {
                tmp = p->value[found][0];
            }
            *(int64_t *)value = tmp;
            *vlen = sizeof(int64_t);

            // unlock the item
            spin_unlock(&(item->lock));
            return 0;
        }
    }

    // not found
    spin_unlock(&(item->lock));
    return -1;
}

int blockTableDel(BaseTable * table, char * key, size_t klen) {
    // tired
    return 0;
}