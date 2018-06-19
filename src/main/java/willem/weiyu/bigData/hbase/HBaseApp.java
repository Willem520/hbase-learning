package willem.weiyu.bigData.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author weiyu
 * @description
 * @create 2018/5/16 13:25
 * @since 1.0.0
 */
public class HBaseApp {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();

        try(Connection conn = ConnectionFactory.createConnection(conf);Admin admin = conn.getAdmin()) {
            //列出hbase中的表
            listTables(admin);
            //创建表
            //createTable(admin);
            //删除表
//            deleteTable(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listTables(Admin admin) throws IOException {
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println("======namespace:"+tableName.getNamespaceAsString()+",tablename:"+tableName.getNameAsString());
        }
    }

    private static void createTable(Admin admin) throws IOException {
        //NamespaceDescriptor namespace = NamespaceDescriptor.create("weiyu").build();
        //admin.createNamespace(namespace);
        TableName tableName = TableName.valueOf("test_hbase");
//        if (admin.tableExists(tableName)){
//            System.out.println("======表已存在：");
//            return;
//        }
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("fm1");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnFamilyDescriptor).build();
        admin.createTable(tableDescriptor);
    }

    private static void deleteTable(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("test_hbase");
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }
}
