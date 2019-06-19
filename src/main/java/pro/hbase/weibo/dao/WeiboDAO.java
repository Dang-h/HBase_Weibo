package pro.hbase.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createNameSpace(String nameSpace) {


    }
}
