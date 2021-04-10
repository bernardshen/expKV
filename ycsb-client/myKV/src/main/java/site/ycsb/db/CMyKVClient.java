package site.ycsb.db;


public class CMyKVClient {
    
    public long CCMPtr;

    public native int cInit();
    public native int cGet1s(long CMptr, String key, int klen, KVReply reply);
    public native int cGet2s(long CMptr, String key, int klen, KVReply reply);
    public native int cPut(long CMptr, String key, int klen, KVReply reply);
    public native int cDel(long CMptr, String key, int klen);

    static {
        System.load("/home/sjc/learn/learn-kv/mykv/build/src/libMyKV.so");
        System.load("/home/sjc/learn/learn-kv/ycsb-src/myKV/src/main/java/site/ycsb/db/libmylib.so");
    }


    public static void main(String [] args) {
        int ret = 0;
        CMyKVClient c = new CMyKVClient();
        c.CCMPtr = 0;
        System.out.print(c.CCMPtr);
        ret = c.cInit();
        if (ret < 0) {
            System.out.println("cInit failed");
            return;
        }

        KVReply reply = new KVReply();
        reply.set(10, 8);
        ret = c.cPut(c.CCMPtr, "key", 3, reply);
        if (ret < 0) {
            System.out.println("cPut failed");
            return;
        }
        
        KVReply nReply = new KVReply();
        ret = c.cGet1s(c.CCMPtr, "key", 3, nReply);
        if (ret < 0) {
            System.out.println("cGet failed");
            return;
        }
        System.out.printf("key: %s, value: %d\n", "key", nReply.value);
    }
}

class KVReply {
    public long value;
    public long vlen;

    public void set(long value, long vlen) {
        this.value = value;
        this.vlen = vlen;
    }
}
