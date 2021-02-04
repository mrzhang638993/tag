package util.com.date.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class KylinTest {
    public static void main(String[] args) throws Exception {
        // 1、加载驱动
        Class.forName("org.apache.kylin.jdbc.Driver");
        // 2、创建Connection连接对象
        // 连接字符串：jdbc:kylin://ip地址:7070/项目名称（project）
        Connection connection = DriverManager.getConnection("jdbc:kylin://hadoop01:7070/bigdata_kylin",
                "ADMIN",
                "KYLIN");

        // 3、创建Statement对象，并执行executeQuery，获取ResultSet
        Statement statement = connection.createStatement();
        // 构建SQL和语句
        String sql = "select\n" +
                "  t1.date1,\n" +
                "  t2.regionname,\n" +
                "  productname,\n" +
                "  sum(t1.price) as total_money,\n" +
                "  sum(t1.amount) as total_amount\n" +
                "from\n" +
                "  dw_sales t1\n" +
                "inner join dim_region t2\n" +
                "on t1.regionid = t2.regionid\n" +
                "inner join dim_product t3\n" +
                "on t1.productid = t3.productid\n" +
                "group by\n" +
                "  t1.date1,\n" +
                "  t2.regionid,\n" +
                "  t2.regionname,\n" +
                "  t3.productid,\n" +
                "  t3.productname";
        ResultSet resultSet = statement.executeQuery(sql);
        // 4、打印ResultSet
        while (resultSet.next()) {
            // 4.1 获取时间
            String date1 = resultSet.getString("date1");
            // 4.2 获取区域名称
            String regionname = resultSet.getString("regionname");
            // 4.3 获取产品名称
            String productname = resultSet.getString("productname");
            // 4.4 总金额
            String total_money = resultSet.getString("total_money");
            // 4.5 总数量
            String total_amount = resultSet.getString("total_amount");
            System.out.println(date1 + " " + regionname + " " + productname + " " + total_money + " " + total_amount);
        }
        connection.close();
    }
}
