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

    public void follow(String fans, String star) throws IOException {

        // 1. 向relation表中插入两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        String time = System.currentTimeMillis() + "";

        dao.putCell(Names.TABLE_RELATION, rowKey1, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);
        dao.putCell(Names.TABLE_RELATION, rowKey2, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);

        // 2 从weibo表中获取star所有weiboId（3条）
        String startRow = star;
        String stopRow = star + "_|";
        List<String> list = dao.getRowKeyByRange(Names.TABLE_WEIBO, startRow, stopRow);

        // 3. 取近期几条
        // 如果微博不足3条，返回0
        int fromIndex = list.size() > Names.INBOX_DATA_VERSIONS ? list.size() - Names.INBOX_DATA_VERSIONS : 0;
        List<String> recentWeibos = list.subList(fromIndex, list.size());

        // 4. 向fans的inbox表种插入star近期weiboId
        // 先插最早的，List中顺序为从早到晚
        for (String recentWeibo : recentWeibos) {
            dao.putCell(Names.TABLE_INBOX, fans, Names.INBOX_FAMILY_DATA, star,recentWeibo);

        }
    }
}
