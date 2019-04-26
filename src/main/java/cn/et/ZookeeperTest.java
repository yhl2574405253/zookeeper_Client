package cn.et;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

import java.sql.*;

public class ZookeeperTest {
    private static byte [] url;
    private static byte [] driverClass;
    private static byte [] userName;
    private static byte [] password;
    private static Connection connction;
    private static ZkClient zk;

    public static void main(String[] args) throws Exception {
//      连接zookeeper服务端
        zk =new ZkClient("localhost:2181",10000,5000,new BytesPushThroughSerializer());
//      从库里面取出已经设定好的数据
        url = zk.readData("/db/url");
        driverClass = zk.readData("/db/driverClass");
        userName = zk.readData("/db/userName");
        password = zk.readData("/db/password");

//      开启一个监控功能
        zk.subscribeDataChanges("/db/url", new IZkDataListener() {
            /**
             * 监控删除
             * @param path
             */
            public void handleDataDeleted(String path){
                System.out.println("监控删除");
            }

            /**
             * 监控修改
             * @param path
             * @param data
             * @throws Exception
             */
            public void handleDataChange(String path, Object data) throws Exception {
                url = zk.readData("/db/url");
                connction = getConnction(new String(url),new String(driverClass),new String(userName),new String(password));
                List();
                System.out.println("=============================");
            }
        });
//      因为要测试监控功能，这个来了一个四循环，不让这个进程结束
        while (true) {
            Thread.sleep(Integer.MAX_VALUE);
        }
    }

//  封装一个连接数据库的Connection
    public static Connection getConnction(String url, String driverClass, String userName, String password) throws Exception{
        Class.forName(driverClass);
        connction = DriverManager.getConnection(url,userName,password);
        return connction;
    }

//  封装一个简单的查询方法
    public static void List() throws Exception{
        String sql ="select * from userinfo";
        PreparedStatement ps=connction.prepareStatement(sql);

        ResultSet rSet =ps.executeQuery();  //执行sql语句
        ResultSetMetaData metaData = rSet.getMetaData();  //new一个getMetaData
        int columnCount = metaData.getColumnCount();  //获取表中的总列数

        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnLabel(i)+"\t");  //获取列名
        }
        System.out.println();
        while (rSet.next()) {
            for (int i = 1; i <=columnCount ; i++) {
                System.out.print(rSet.getString(i)+"\t");
            }
            System.out.println();
        }
    }
}
