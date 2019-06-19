package pro.hbase.weibo;

import pro.hbase.weibo.controller.WeiboController;

import java.io.IOException;

public class WeiboApp {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

        // 初始化,创建表：weibo,relation,inbox
        controller.init();

    }


}
