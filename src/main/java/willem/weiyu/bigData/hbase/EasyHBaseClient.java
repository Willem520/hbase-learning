package willem.weiyu.bigData.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author weiyu
 * @description
 * @create 2018/5/16 13:25
 * @since 1.0.0
 */
public class EasyHBaseClient {
    private Logger log = LoggerFactory.getLogger(getClass());

    private static final String ROW_FORMAT = "{\"rowKey\":\"%s\", \"columnFamily\":\"%s\", \"column\":\"%s\", \"value\":\"%s\", \"timestamp\":\"%s\"}";
    private final Connection conn;

    private static final int BUFFER_SIZE = 1024*1024*4;

    public static class Builder {
        public static EasyHBaseClient create() throws IOException {
            return new EasyHBaseClient();
        }
    }

    private EasyHBaseClient() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.26.27.81");
        conn = ConnectionFactory.createConnection(conf);
    }

    public void listTables() throws IOException {
        Admin admin = conn.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            log.info("******namespace:{}, tablename:{}", tableName.getNamespaceAsString(), tableName.getNameAsString());
            System.out.println(String.format("******namespace:%s, tablename:%s", tableName.getNamespaceAsString(), tableName.getNameAsString()));
        }
    }

    public void createTable(String tableName) throws IOException {
        //设置表名
        TableName tableN = TableName.valueOf(tableName);
        Admin admin = conn.getAdmin();
        if (admin.tableExists(tableN)) {
            log.warn("******表已存在:{}", tableN.getNameAsString());
            System.out.println(String.format("******表已存在:%s", tableN.getNameAsString()));
            return;
        }
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("fm1").setVersions(3, 5);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableN).addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }

    public void deleteTable(String tableName) throws IOException {
        TableName tableN = TableName.valueOf(tableName);
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableN)) {
            log.warn("******表{}不存在", tableName);
            return;
        }
        admin.disableTable(tableN);
        admin.deleteTable(tableN);
        log.info("******表已删除:{}", tableName);
        System.out.println(String.format("******表已删除:%s", tableName));
    }

    public void add(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public void batchPut(String tableName, List<Put> puts) throws IOException {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .listener((exception, mutator1) -> {
                    for (int i = 0; i < exception.getNumExceptions(); i++) {
                        System.out.println("Failed to sent put " + exception.getRow(i) + ".");
                    }
                }).writeBufferSize(BUFFER_SIZE);
        BufferedMutator mutator = conn.getBufferedMutator(params);
        mutator.mutate(puts);
        mutator.flush();
        mutator.close();
    }

    public void get(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        if (result == null) {
            log.warn("******rowKey[{}]在table[{}]中不存在", rowKey, tableName);
            System.out.println(String.format("******rowKey[%s]在table[%s]中不存在", rowKey, tableName));
            return;
        }
        show(result);
    }

    public void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        if (colFamily != null && col != null) {
            delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        } else if (colFamily != null && col == null) {
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

    public void stop() {
        try {
            if (conn != null)
                conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
