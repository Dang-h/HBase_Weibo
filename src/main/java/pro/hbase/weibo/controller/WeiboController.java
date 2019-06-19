package pro.hbase.weibo.controller;

import pro.hbase.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

public class WeiboController {

    private WeiboService service = new WeiboService();

    public void init() throws IOException {
        service.init();
    }

    //5) 发布微博内容
    public void publish(String star, String content) {

    }

    //6) 添加关注用户
    public void follow(String fans, String star) {

    }

    //7) 移除（取关）用户
    public void unFollow(String fans, String star) {

    }

    //8) 获取关注的人的微博内容
    // 8.1 获取某个明星的所有weibo
    public List<String> getAllWeiboByUserId(String star) {
        return null;
    }

    // 8.2 获取关注的所有star的近期weibo（3条）
    public List<String> getAllRecentWeibos(String fans) {
        return null;
    }

}
