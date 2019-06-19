package pro.hbase.weibo.service;


import pro.hbase.weibo.constant.Names;
import pro.hbase.weibo.dao.WeiboDAO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


    public void publish(String star, String content) throws IOException {

        // 1. 在weibo表插入数据
        String rowKey = star + "_" + System.currentTimeMillis();
        dao.putCell(Names.TABLE_WEIBO, rowKey, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT, content);

        // 2. 获取relation获取star所有fansId
        //prefix => star:followedby:fans
        String prefix = star + ":followedby:";
        List<String> list = dao.getRowKeyByPrefix(Names.TABLE_RELATION, prefix);

        // 如果没有粉丝，return
        if (list.size() == 0) {
            return;
        }

        // 4
        ArrayList<String> fansId = new ArrayList<>();

        // 3. 切分
        for (String row : list) {
            String[] split = row.split(":");

            // 获取id
            fansId.add(split[2]);
        }

        // 5. 想所有fans的inbox中插入本条weibo的Id;
        // 每一个fansId，相同的列族，相同的列，相同的内容
        // 往多行插入相同的列,批量插入
        dao.putCells(Names.TABLE_INBOX, fansId, Names.INBOX_FAMILY_DATA, star, rowKey);

    }
}
