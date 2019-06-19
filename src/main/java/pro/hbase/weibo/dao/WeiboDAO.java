package pro.hbase.weibo.dao;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;

public class WeiboDAO {

    public static Connection connection = null;

    static {

        try {
            Configuration conf = HBaseConfiguration.create();
            //设置zookeeper
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace
     * @throws IOException
     */
    public void createNameSpace(String nameSpace) throws IOException {

        // 1. 获取admin对象:用于创建、删除、列表、启用和禁用表、添加和删除表列族和其他管理操作。
        Admin admin = connection.getAdmin();

        try {
            // 3. 创建namespace,NamespaceDescriptor为私有构造器，不可new
            NamespaceDescriptor namespace = NamespaceDescriptor.create(nameSpace).build();

            // 2
            admin.createNamespace(namespace);
        } finally {

            // 4
            admin.close();
        }
    }

    public void createTable(String tableName, String... families) throws IOException {

        createTable(tableName, 1, families);

    }

    public void createTable(String tableName, Integer versions, String... families) throws IOException {

        // 1.
        Admin admin = connection.getAdmin();

        try {
            // 3.
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

            // 4.
            for (String family : families) {

                // 6.
                HColumnDescriptor familyDesc = new HColumnDescriptor(family);

                // 7. 设置版本数
                familyDesc.setMaxVersions(versions);

                // 5
                table.addFamily(familyDesc);
            }

            // 2.
            admin.createTable(table);
        } finally {

            // 8.
            admin.close();
        }

    }
}
