package cn.et;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public class Test1 {
    public static void main(String[] args) throws Exception {
        final ZkClient zk =new ZkClient("localhost:2181",10000,5000,new BytesPushThroughSerializer());
        zk.subscribeDataChanges("/test", new IZkDataListener() {
            public void handleDataChange(String s, Object o){
                byte [] test = zk.readData("/test");
                System.out.println(new String(test));
            }
            public void handleDataDeleted(String s) {

            }
        });
        while (true) {
            Thread.sleep(Integer.MAX_VALUE);
        }
    }
}
