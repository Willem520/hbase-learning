package willem.weiyu.bigData.phoenix;

import java.io.IOException;
import java.sql.*;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/1/3 20:23
 */
public class EasyPhoenixClient {
    private Connection conn;

    private EasyPhoenixClient()throws ClassNotFoundException, SQLException{
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection("jdbc:phoenix:10.26.27.81:2181");
    }

    public static class Builder{
        public static EasyPhoenixClient create() throws IOException, SQLException, ClassNotFoundException {
            return new EasyPhoenixClient();
        }
    }

    public void list(String tableName) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select * from "+tableName);
        //pstmt.setString(1,tableName);
        ResultSet rst = pstmt.executeQuery();
        while (rst.next()){
            System.out.println(rst.getString(0));
        }
    }

    public void create() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("CREATE TABLE test_phoenix_api(mykey integer not null primary key ,mycolumn varchar )");
        pstmt.execute();
        conn.commit();
    }
}
