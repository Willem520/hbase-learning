package willem.weiyu.bigData.hbase;

import org.junit.Test;

import java.io.IOException;

/**
 * @author weiyu
 * @description
 * @create 2018/6/20 15:00
 * @since 1.0.0
 */
public class HBaseAppTest {
    private HBaseApp hbase;

    public HBaseAppTest() throws IOException {
        hbase = new HBaseApp();
    }

    @Test
    public void testList() throws IOException {
        hbase.listTables();
    }

    @Test
    public void testCreate() throws IOException {
        hbase.createTable();
    }

    @Test
    public void testDelete() throws IOException {
        hbase.deleteTable();
    }

    @Test
    public void testAdd() throws IOException {
        hbase.add("weiyu_hbase","0001","fm1","name","weiyu");
    }

    @Test
    public void testGet() throws IOException {
        hbase.get("weiyu_hbase","0001");
    }
}
