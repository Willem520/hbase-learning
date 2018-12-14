package willem.weiyu.bigData.hbase;

import org.junit.Test;

import java.io.IOException;

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
        EasyHBaseClient.Builder.create().add("weiyu_hbase","0001","fm1","name","weiyu");
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
