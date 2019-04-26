package cn.et;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public class Test2 {
    public static void main(String[] args) {
        final ZkClient zk =new ZkClient("localhost:2181",10000,5000,new BytesPushThroughSerializer());
        byte [] bytes = "jdbc:mysql://localhost:3306/test".getBytes();
        zk.writeData("/db/url",bytes);
    }
}
