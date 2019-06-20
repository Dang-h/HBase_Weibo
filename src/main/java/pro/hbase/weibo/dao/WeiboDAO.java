package pro.hbase.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pro.hbase.weibo.constant.Names;

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

    /**
     * 创建weibo和relation表
     *
     * @param tableName
     * @param families
     * @throws IOException
     */
    public void createTable(String tableName, String... families) throws IOException {

        createTable(tableName, 1, families);

    }

    /**
     * 创建weibo内容接收inbox表
     *
     * @param tableName
     * @param versions
     * @param families
     * @throws IOException
     */
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


    /**
     * 在weibo表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
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

    /**
     * 从relation表中获取star的fansId
     *
     * @param tableName
     * @param prefix
     * @return
     * @throws IOException
     */
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

    /**
     * 向所有fans的inbox中插入本条weibo的Id
     *
     * @param tablName
     * @param rowKeys
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
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

    /**
     * 从weibo表中获取star所有weiboId
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public List<String> getRowKeyByRange(String tableName, String startRow, String stopRow) throws IOException {

        // 1.
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 7.
        ArrayList<String> list = new ArrayList<>();
        ResultScanner scanner = null;

        try {
            // 3.
            Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));

            // 2.
            scanner = table.getScanner(scan);

            // 4.
            for (Result result : scanner) {
                // 5.
                byte[] row = result.getRow();
                // 6.
                String rowKey = Bytes.toString(row);
                // 8.
                list.add(rowKey);
            }
        } finally {

            // 9.
            table.close();
            scanner.close();
        }


        return list;

    }

    /**
     * 删除relation表中两条数据；
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 删除inbox的一列
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public void deleteCells(String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
            table.delete(delete);
        } finally {

            table.close();
        }
    }

    /**
     * 根据rowKey获取多行中同一列的值
     *
     * @param tableName
     * @param prefix
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<String> list;
        ResultScanner scanner = null;
        try {
            list = new ArrayList<>();
            Scan scan = new Scan();
            // 前缀过滤器
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));
            // 指定要获取的列
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                // 克隆value
                list.add(Bytes.toString(CellUtil.cloneValue(cells[0])));
            }
        } finally {

            scanner.close();
            table.close();
        }
        return list;
    }

    /**
     * 根据rowKey获取一个列族的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     * @throws IOException
     */
    public List<String> getFamilyByRowKey(String tableName, String rowKey, String family) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<String> list = new ArrayList<>();

        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setMaxVersions(Names.INBOX_DATA_VERSIONS);
            get.addFamily(Bytes.toBytes(family));
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
                list.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } finally {

            table.close();
        }
        return list;
    }

    /**
     * 获取多行中相同列的数据
     *
     * @param tableName
     * @param rowKeys
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public List<String> getCellsByRowKey(String tableName, List<String> rowKeys, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<String> weibos = null;
        try {
            ArrayList<Get> gets = new ArrayList<>();
            weibos = new ArrayList<>();
            for (String rowKey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowKey));
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
                gets.add(get);
            }

            Result[] results = table.get(gets);
            for (Result result : results) {
                String weibo = Bytes.toString(CellUtil.cloneValue(result.rawCells()[0]));
                weibos.add(weibo);
            }
        } finally {

            table.close();
        }

        return weibos;
    }
}
