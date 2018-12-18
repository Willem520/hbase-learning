package willem.weiyu.bigData.hbase;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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
    public void testGet() throws IOException {
        EasyHBaseClient.Builder.create().get("test","1");
    }

    @Test
    public void testScan() throws IOException {
        EasyHBaseClient.Builder.create().scan("test");
    }
}
