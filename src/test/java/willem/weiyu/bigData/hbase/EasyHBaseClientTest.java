package willem.weiyu.bigData.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author weiyu
 * @description
 * @create 2018/6/20 15:00
 * @since 1.0.0
 */
public class EasyHBaseClientTest {

    @Test
    public void testList() throws IOException {
        EasyHBaseClient.Builder.create().listTables();
    }

    @Test
    public void testCreate() throws IOException {
        EasyHBaseClient.Builder.create().createTable("test");
    }

    @Test
    public void testDelete() throws IOException {
        EasyHBaseClient.Builder.create().deleteTable("");
    }

    @Test
    public void testAdd() throws IOException {
        EasyHBaseClient  hBaseClient = EasyHBaseClient.Builder.create();
        Random rand = new Random();
        String separator = "_";
        DecimalFormat df = new DecimalFormat("0.00");
        for (int i=1; i<=5; i++){
            String key = "000"+i;
            key = key.hashCode() + separator +"20181218"+ separator + key;
            hBaseClient.add("test",key,"fm1","score",df.format(rand.nextDouble()*100));
            hBaseClient.add("test",key,"fm2","rank",i+"");
        }
    }

    @Test
    public void testBatchPut() throws IOException {
        EasyHBaseClient  hBaseClient = EasyHBaseClient.Builder.create();
        Random rand = new Random();
        String separator = "_";
        DecimalFormat df = new DecimalFormat("0.00");
        List<Put> puts = new ArrayList<>();

        for (int i=1; i<=10000; i++){
            String key = String.format("%08d",i);
            key = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))+ separator + key;
            Put put = new Put(Bytes.toBytes(key));
            put.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("score"), Bytes.toBytes(df.format(rand.nextDouble()*100)));
            puts.add(put);
        }
        hBaseClient.batchPut("test", puts);
    }

    @Test
    public void testGet() throws IOException {
        EasyHBaseClient.Builder.create().get("test","1");
    }

    @Test
    public void testScan() throws IOException {
        EasyHBaseClient.Builder.create().scan("test");
    }

    @Test
    public void test(){
        System.out.println(String.format("%08d",12));
    }
}
