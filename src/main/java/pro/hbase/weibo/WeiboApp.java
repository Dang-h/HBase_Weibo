package pro.hbase.weibo;

import pro.hbase.weibo.controller.WeiboController;

import java.io.IOException;

public class WeiboApp {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

        // 初始化,创建表：weibo,relation,inbox
//        controller.init();

        // 发布微博(添加数据到表weibo：weibo)
//        controller.publish("1001", "happy 1");
//        controller.publish("1001", "happy 2");
//        controller.publish("1001", "happy 3");
//        controller.publish("1001", "happy 4");
//        controller.publish("1001", "happy 5");

        // 添加关注用户
//        controller.follow("aa", "bb");
//        controller.follow("cc", "aa");
//        controller.follow("bb", "aa");
        controller.follow("1002", "1001");
        controller.follow("1003", "1001");
    }


}
