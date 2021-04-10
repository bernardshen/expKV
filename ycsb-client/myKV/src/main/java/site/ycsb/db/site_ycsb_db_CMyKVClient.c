#include "site_ycsb_db_CMyKVClient.h"
#include "RPCClient.h"
#include <string.h>
#include <assert.h>

static inline jlong addr2java(void * p) {
    assert(p == (void *)(uintptr_t)p);
    return (uintptr_t)p;
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_CMyKVClient_cInit(JNIEnv * jenv, jobject kvobj) {
    RPCClient * client = (RPCClient *)malloc(sizeof(RPCClient));
    int ret = -1;
    jlong ptr = 0;
    ret = initRPCClient(client, "10.176.37.63", SIMPLE);
    if (ret < 0) {
        printf("initRPCClient failed\n");
        return -1;
    }
    jclass clazz = (*jenv)->GetObjectClass(jenv, kvobj);
    jfieldID fid = (*jenv)->GetFieldID(jenv, clazz, "CCMPtr", "J");
    (*jenv)->SetLongField(jenv, kvobj, fid, addr2java((void *)client));
    return 0; // return success here
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_CMyKVClient_cGet1s
    (JNIEnv * jenv, jobject jobj, jlong jCMPtr, jstring jkey, jint jklen, jobject jKVReply) {
    int ret = -1;
    // get RPCClient pointer
    RPCClient * client = (RPCClient *)(uintptr_t)jCMPtr;

    // get key
    jboolean isCopy;
    char * buf = (char *)(*jenv)->GetStringUTFChars(jenv, jkey, &isCopy);

    // prepare value
    int64_t value = 0;
    uint64_t vlen = 0;

    // get remote value
    ret = RPCClientKVGet1S(client, buf, (uint64_t)jklen, &value, &vlen);
    if (ret < 0) {
        printf("RPCClientKVGet1S failed\n");
        return -1;
    }

    // release buf and prepare kvreply
    (*jenv)->ReleaseStringUTFChars(jenv, jkey, buf);
    jclass clazz = (*jenv)->GetObjectClass(jenv, jKVReply);
    jfieldID valueFid = (*jenv)->GetFieldID(jenv, clazz, "value", "J");
    jfieldID vlenFid = (*jenv)->GetFieldID(jenv, clazz, "vlen", "J");
    (*jenv)->SetLongField(jenv, jKVReply, valueFid, (jlong)value);
    (*jenv)->SetLongField(jenv, jKVReply, vlenFid, (jlong)vlen);

    return 0; // return success here
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_CMyKVClient_cGet2s
  (JNIEnv * jenv, jobject jobj, jlong jCMPtr, jstring jkey, jint jklen, jobject jKVReply) {
    int ret = -1;
    // get RPCClient pointer
    RPCClient * client = (RPCClient *)(uintptr_t)jCMPtr;

    // get key
    jboolean isCopy;
    char * buf = (char *)(*jenv)->GetStringUTFChars(jenv, jkey, &isCopy);

    // prepare value
    int64_t value = 0;
    uint64_t vlen = 0;

    // get remote value
    ret = RPCClientKVGet2S(client, buf, (uint64_t)jklen, &value, &vlen);
    if (ret < 0) {
        printf("RPCClientKVGet1S failed\n");
        return -1;
    }

    // release buf and prepare kvreply
    (*jenv)->ReleaseStringUTFChars(jenv, jkey, buf);
    jclass clazz = (*jenv)->GetObjectClass(jenv, jKVReply);
    jfieldID valueFid = (*jenv)->GetFieldID(jenv, clazz, "value", "J");
    jfieldID vlenFid = (*jenv)->GetFieldID(jenv, clazz, "vlen", "J");
    (*jenv)->SetLongField(jenv, jKVReply, valueFid, (jlong)value);
    (*jenv)->SetLongField(jenv, jKVReply, vlenFid, (jlong)vlen);

    return 0; // return success here
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_CMyKVClient_cPut
  (JNIEnv * jenv, jobject jobj, jlong jCMPtr, jstring jkey, jint jklen, jobject jKVReply) {
    int ret = -1;
    // get RPCClient pointer
    RPCClient * client = (RPCClient *)(uintptr_t)jCMPtr;

    // get key
    jboolean isCopy;
    char * buf = (char *)(*jenv)->GetStringUTFChars(jenv, jkey, &isCopy);

    // prepare value
    int64_t value = 0;
    uint64_t vlen = 0;

    // get value from kvreply
    jclass clazz = (*jenv)->GetObjectClass(jenv, jKVReply);
    jfieldID valueFid = (*jenv)->GetFieldID(jenv, clazz, "value", "J");
    jfieldID vlenFid = (*jenv)->GetFieldID(jenv, clazz, "vlen", "J");
    value = (int64_t)(*jenv)->GetLongField(jenv, jKVReply, valueFid);
    vlen = (uint64_t)(*jenv)->GetLongField(jenv, jKVReply, vlenFid);

    // put
    ret = RPCClientKVPut(client, buf, (uint64_t)jklen, &value, vlen);
    if (ret < 0) {
        printf("RPCClientKVPut failed\n");
        return -1;
    }
    
    // free char here
    (*jenv)->ReleaseStringUTFChars(jenv, jkey, buf);

    return 0; // return success here
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_CMyKVClient_cDel
  (JNIEnv * jenv, jobject jobj, jlong jCMPtr, jstring jkey, jint jklen) {
    int ret = -1;
    // get RPCClient
    RPCClient * client = (RPCClient *)(uintptr_t)jCMPtr;

    // get key
    jboolean isCopy;
    char * buf = (char *)(*jenv)->GetStringUTFChars(jenv, jkey, &isCopy);

    // call del
    ret = RPCClientKVDel(client, buf, (uint64_t)jklen);
    if (ret < 0) {
        printf("RPCClientKVDel failed\n");
        return -1;
    }
    
    // release cstring
    (*jenv)->ReleaseStringUTFChars(jenv, jkey, buf);

    return 0; // return success here
}