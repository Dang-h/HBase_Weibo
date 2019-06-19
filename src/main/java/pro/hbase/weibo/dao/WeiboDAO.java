package pro.hbase.weibo.dao;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


    public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {

        // 1.
        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            // 3
            Put put = new Put(Bytes.toBytes(rowKey));

            // 4
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

            // 2.
            table.put(put);
        } finally {

            // 5
            table.close();
        }

    }

    public List<String> getRowKeyByPrefix(String tableName, String prefix) throws IOException {

        // 8
        ArrayList<String> list = new ArrayList<>();

        // 1
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = null;

        try {
            // 3
            Scan scan = new Scan();

            // 4 传入前缀
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));

            // 2 scanner中有多行数据
            scanner = table.getScanner(scan);

            // 5 获取rowKey
            // 一个result一行，一行中可能有多个列，多列的rowKey相同
            for (Result result : scanner) {
                // 6 获取一行的rowKey
                byte[] row = result.getRow();

                // 7 字节数组转String，获得String类型rowKey
                String rowKey = Bytes.toString(row);

                // 9 把rowKey放入list
                list.add(rowKey);
            }
        } finally {

            // 10
            scanner.close();
            table.close();

        }

        return list;

    }

    public void putCells(String tablName, ArrayList<String> rowKeys, String family, String column, String value) throws IOException {

        // 1
        Table table = connection.getTable(TableName.valueOf(tablName));

        try {
            // 3 创建List对象
            ArrayList<Put> puts = new ArrayList<>();

            // 4 遍历rowKeys同时创建put对象，再把put放入List<Put>
            for (String rowKey : rowKeys) {

                //5
                Put put = new Put(Bytes.toBytes(rowKey));

                //7
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

                //6
                puts.add(put);
            }

            // 2. 传入List<Put>
            table.put(puts);

        } finally {

            //8
            table.close();
        }

    }
}
