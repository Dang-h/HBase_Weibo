package pro.hbase.weibo;

import pro.hbase.weibo.controller.WeiboController;

import java.io.IOException;

public class WeiboApp {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

        // 初始化,创建表：weibo,relation,inbox
//        controller.init();

        // 发布微博(添加数据到表weibo：weibo)
        controller.publish("1001", "happy 1");
        controller.publish("1001", "happy 2");
        controller.publish("1001", "happy 3");
        controller.publish("1001", "happy 4");
        controller.publish("1001", "happy 5");
    }


}
