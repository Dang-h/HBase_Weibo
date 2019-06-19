package pro.hbase.weibo.service;


import pro.hbase.weibo.constant.Names;
import pro.hbase.weibo.dao.WeiboDAO;

import java.io.IOException;

public class WeiboService {

    private WeiboDAO dao = new WeiboDAO();

    public void init() throws IOException {

        // 1. 创建命名空间及表名的定义
        dao.createNameSpace(Names.NAMESPACE_WEIBO);

        // 2. 创建微博内容表
        dao.createTable(Names.TABLE_WEIBO, Names.WEIBO_FAMILY_DATA);

        // 3. 创建用户关系表
        dao.createTable(Names.TABLE_RELATION, Names.RELATION_FAMILY_DATA);

        // 4. 创建用户微博内容接收邮件表
        dao.createTable(Names.TABLE_INBOX, Names.INBOX_DATA_VERSIONS, Names.INBOX_FAMILY_DATA);
    }
}
