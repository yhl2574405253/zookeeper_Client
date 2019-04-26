
package cn.et;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 实现一个zookeeper的客户端
 *
 * @author jiaozi
 *
 */
public class TestMain implements Watcher {
    /**
     * 历史记录版本号
     */
    private static int version = 1;
    /**
     * 版本和历史记录的sql语句
     * 比如
     *    1 set /a 1
     *    2 get /a
     */
    private static Map<Integer,String> versionList=new HashMap<Integer,String>();
    /**
     * 连接zookeeper对象
     */
    private static ZooKeeper zook = null;
    /**
     * ip:端口的字符串
     */
    private static String ipPort = null;
    /**
     * 是否连接成功
     */
    private static boolean ifConnect = false;

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
//		ipPort = "192.168.58.1:2181";
//		zook = new ZooKeeper(ipPort, 3000, new TestMain());
//		synchronized (zook) {
//			zook.wait();
//		}
        while (true) {
            // 未连接 和已连接的前缀字符串不一样
            if (zook == null)
                System.out.print(">");
            else
                System.out.print("[zk: " + ipPort + "(CONNECTED) " + version + "]");
            String commandStr = sc.nextLine();
            commandStr = commandStr.replaceAll(" +", " ");
            command(commandStr);

        }
    }
    /**
     * 处理命令的逻辑
     * @param command
     */
    public static void command(String command){
        try {
            //记录历史
            versionList.put(version, command);
            version++;
            // connect ip：端口连接到zookeeper的服务
            if (command.startsWith("connect")) {
                ipPort=command.split(" ")[1];
                zook=new ZooKeeper(ipPort, 5000, new TestMain());
                synchronized (zook) {
                    zook.wait();
                }
            } else {
                if (zook == null) {
                    System.out.println("请输入连接命令 connect ip：端口");
                }
                // ls znode路径 查看znode下所有子节点
                if (command.startsWith("ls")) {
                    String[] all = command.split(" ");
                    String znode = all[1];
                    // 参数二表示是否监听 该检点下的子节点的变化 如果true process的watch方法
                    // 在监听到子节点变化会自动触发
                    // event.getType()=EventType.NodeChildrenChanged判断是否是子节点变化事件
                    // event.getPath获取变化的节点 事件只能监听一次 触发后就不会监听了
                    List allChildren = zook.getChildren(znode, false);
                    System.out.println(allChildren);
                }
                // create [-s|-e] znode路径 值 acl权限
                if (command.startsWith("create")) {
                    CreateMode createMode = getMode(command);
                    command = command.replaceAll("-e ", "");
                    command = command.replaceAll("-s ", "");
                    String[] all = command.split(" ");
                    String znode = all[1];
                    String zvalue = all[2];
                    // String acl=all[3];
                    // 异步调用
                    String name = zook.create(znode, zvalue.getBytes(), Ids.OPEN_ACL_UNSAFE, createMode);
                    System.out.println("Created " + name);
                }
                // set znode路径 值
                /**
                 * [zk: localhost:2181(CONNECTED) 24] set /a 2 cZxid =
                 * 0x300000039 ctime = Tue May 16 17:20:05 PDT 2017 mZxid =
                 * 0x30000003b mtime = Tue May 16 17:31:39 PDT 2017 pZxid =
                 * 0x300000039 cversion = 0 dataVersion = 1 aclVersion = 0
                 * ephemeralOwner = 0x0 dataLength = 1 numChildren = 0
                 */
                if (command.startsWith("set")) {
                    String[] all = command.split(" ");
                    String znodePath = all[1];
                    String znodeValue = all[2];
                    Stat stat = zook.setData(znodePath, znodeValue.getBytes(), -1);
                    System.out.println("cZxid = " + stat.getCzxid());
                    System.out.println("ctime = " + stat.getCtime());
                    System.out.println("mZxid = " + stat.getMzxid());
                    System.out.println("mtime = " + stat.getMtime());
                    System.out.println("pZxid = " + stat.getPzxid());
                    System.out.println("cversion = " + stat.getCversion());
                    System.out.println("dataVersion = " + stat.getVersion());
                    System.out.println("aclVersion = " + stat.getAversion());
                    System.out.println("ephemeralOwner = " + stat.getEphemeralOwner());
                    System.out.println("dataLength = " + stat.getDataLength());
                    System.out.println("numChildren = " + stat.getNumChildren());
                }

                // get znode路径
                /**
                 * [zk: localhost:2181(CONNECTED) 24] get /a 3 cZxid =
                 * 0x300000039 ctime = Tue May 16 17:20:05 PDT 2017 mZxid =
                 * 0x30000003b mtime = Tue May 16 17:31:39 PDT 2017 pZxid =
                 * 0x300000039 cversion = 0 dataVersion = 1 aclVersion = 0
                 * ephemeralOwner = 0x0 dataLength = 1 numChildren = 0
                 */
                if (command.startsWith("get") || command.startsWith("stat")) {
                    String[] all = command.split(" ");
                    String znodePath = all[1];
                    Stat stat = new Stat();
                    if (!command.startsWith("stat")) {
                        byte[] val = zook.getData(znodePath, false, stat);
                        System.out.println(new String(val));
                    }
                    System.out.println("cZxid = " + stat.getCzxid());
                    System.out.println("ctime = " + stat.getCtime());
                    System.out.println("mZxid = " + stat.getMzxid());
                    System.out.println("mtime = " + stat.getMtime());
                    System.out.println("pZxid = " + stat.getPzxid());
                    System.out.println("cversion = " + stat.getCversion());
                    System.out.println("dataVersion = " + stat.getVersion());
                    System.out.println("aclVersion = " + stat.getAversion());
                    System.out.println("ephemeralOwner = " + stat.getEphemeralOwner());
                    System.out.println("dataLength = " + stat.getDataLength());
                    System.out.println("numChildren = " + stat.getNumChildren());
                }
                //delete znode路径 删除节点
                if (command.startsWith("delete")) {
                    String[] all = command.split(" ");
                    String znodePath = all[1];
                    zook.delete(znodePath, -1);
                }
                //history 查看历史记录
                if (command.startsWith("history")) {
                    for(Map.Entry<Integer,String> me:versionList.entrySet()){
                        System.out.println(me.getKey()+" "+me.getValue());
                    }
                }
                //redo 版本号 重新执行历史记录版本号对应的命令
                if (command.startsWith("redo")) {
                    String[] all = command.split(" ");
                    String commandNo = all[1];
                    command=versionList.get(Integer.parseInt(commandNo));
                    if(command!=null){
                        command(command);
                    }
                }
                //close退出到没登录状态
                if (command.startsWith("close")) {
                    zook.close();
                    zook=null;
                }
                //quit退出程序
                if (command.startsWith("quit")) {
                    zook.close();
                    zook=null;
                    System.exit(0);
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    /**
     * 通过create命令 判断是哪一种节点类型 //没有 -e -s表示持久节点 // -e 表示临时节点 // -s 表示持久顺序节点 // -e
     * -s 表示临时顺序节点
     *
     * @param str
     * @return
     */
    public static CreateMode getMode(String str) {
        CreateMode cm = CreateMode.PERSISTENT;
        ;
        if (str.indexOf("-") >= 0) {
            if (str.indexOf("-e") >= 0) {
                if (str.indexOf("-s") < 0) {
                    cm = CreateMode.EPHEMERAL;
                } else {
                    cm = CreateMode.EPHEMERAL_SEQUENTIAL;
                }
            } else {
                if (str.indexOf("-s") >= 0) {
                    cm = CreateMode.PERSISTENT_SEQUENTIAL;
                }
            }
        }
        return cm;
    }

    public void process(WatchedEvent event) {
        // 表示已经连接
        if (event.getState() == KeeperState.SyncConnected) {
            ifConnect = true;
            synchronized (zook) {
                zook.notifyAll();
            }

        }
        // System.out.println("状态:"+event.getState());
    }

}