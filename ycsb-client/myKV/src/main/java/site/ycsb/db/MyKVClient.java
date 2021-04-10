package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.Set;
import java.util.Map;
import java.util.Vector;
import java.util.HashMap;

public class MyKVClient extends DB {
    CMyKVClient cClient;

    public void init() throws DBException {
        this.cClient = new CMyKVClient();
        int ret = -1;
        ret = cClient.cInit();
        if (ret < 0) {
            throw new DBException();
        }
    }

    public void cleanup() throws DBException {
        return;
    }

    @Override
    public Status read(String table, String key, 
        Set<String> fields, Map<String, ByteIterator> result) {
        int ret = -1;
        int klen = key.length();
        if (klen > 16) {
            key = key.substring(0, 15);
            klen = 16;
        }
        KVReply reply = new KVReply();
        ret = cClient.cGet1s(cClient.CCMPtr, key, klen, reply);
        if (ret < 0) {
            System.out.println("cGet failed");
            return Status.ERROR;
        }
        String value = "" + reply.value;
        result.put(key, new StringByteIterator(value));
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key,
        Map<String, ByteIterator> values) {
        int ret = -1;
        int klen = key.length();
        KVReply reply = new KVReply();

        if (klen > 16) {
            key = key.substring(0, 15);
            klen = 16;
        }

        // prepare reply
        reply.value = 10;
        reply.vlen = 8;
        ret = cClient.cPut(cClient.CCMPtr, key, klen, reply);
        if (ret < 0) {
            System.out.println("cPut failed");
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        int ret = -1;
        int klen = key.length();

        if (klen > 16) {
            key = key.substring(0, 15);
            klen = 16;
        }

        ret = cClient.cDel(cClient.CCMPtr, key, klen);
        if (ret < 0) {
            System.out.println("cDel failed");
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status update(String table, String key,
        Map<String, ByteIterator> values) {
        int ret = -1;
        int klen = key.length();
        KVReply reply = new KVReply();
        reply.value = 100;
        reply.vlen = 8;

        if (klen > 16) {
            key = key.substring(0, 15);
            klen = 16;
        }

        ret = cClient.cPut(cClient.CCMPtr, key, klen, reply);
        if (ret < 0) {
            System.out.println("cPut failed\n");
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fileds, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }
}