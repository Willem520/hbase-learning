package willem.weiyu.bigData.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author weiyu
 * @description
 * @create 2018/5/16 13:25
 * @since 1.0.0
 */
public class HBaseApp {
    private static final String ROW_FORMAT = "{\"rowKey\":\"%s\", \"columnFamily\":\"%s\", \"column\":\"%s\", \"value\":\"%s\", \"timestamp\":\"%s\"}";
    private final Connection conn;
    private final Admin admin;

    public HBaseApp() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public static void main(String[] args) throws IOException {
        HBaseApp hbase = new HBaseApp();
        //列出所有表
//        hbase.listTables();
        //添加数据
        //hbase.add("weiyu_hbase","0001","fm1","age","111");
        // 查询数据
        // hbase.get("weiyu_hbase","0001");
        hbase.scan("weiyu_hbase");
        // 删除数据
//        hbase.delete("weiyu_hbase","0001","fm1","age");
    }

    public void listTables() throws IOException {
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println("======namespace:" + tableName.getNamespaceAsString() + ",tablename:" + tableName.getNameAsString());
        }
    }

    public void createTable() throws IOException {
        //设置表名
        TableName tableName = TableName.valueOf("weiyu_hbase");
        if (admin.tableExists(tableName)) {
            System.out.println("======表已存在：" + tableName.getNameAsString());
            return;
        }
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("fm1").setVersions(3, 5);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName).addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }

    public void deleteTable() throws IOException {
        TableName tableName = TableName.valueOf("weiyu_hbase");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("======表已删除");
        }
    }

    public void add(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public void get(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        if (result == null) {
            System.out.println("======rowKey为" + rowKey + "的数据不存在");
            return;
        }
        show(result);
    }

    public void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        if (colFamily != null && col != null){
            delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        }else if (colFamily != null && col == null){
            delete.addFamily(Bytes.toBytes(colFamily));
        }
        table.delete(delete);
    }

    public void scan(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            show(result);
        }
        table.close();
    }

    private void show(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(String.format(ROW_FORMAT,
                    new String(CellUtil.cloneRow(cell)),
                    new String(CellUtil.cloneFamily(cell)),
                    new String(CellUtil.cloneQualifier(cell)),
                    new String(CellUtil.cloneValue(cell)),
                    cell.getTimestamp()
            ));
        }
    }
}
