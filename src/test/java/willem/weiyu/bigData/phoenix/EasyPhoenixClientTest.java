package willem.weiyu.bigData.phoenix;

import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/1/3 20:56
 */
public class EasyPhoenixClientTest {

    @Test
    public void test() throws SQLException, IOException, ClassNotFoundException {
        EasyPhoenixClient.Builder.create().list("test_phoenix_api");
    }

    @Test
    public void testCreate() throws SQLException, IOException, ClassNotFoundException {
        EasyPhoenixClient.Builder.create();
    }
}
