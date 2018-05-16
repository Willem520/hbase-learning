package com.weiyu.bigData.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author weiyu
 * @description
 * @create 2018/5/16 13:25
 * @since 1.0.0
 */
public class HBaseApp {

    public static void main(String[] args) {
        System.out.println("======");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://gmbdc-test/apps/hbase/data");

        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println("======tablename:"+tableName.getName().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
