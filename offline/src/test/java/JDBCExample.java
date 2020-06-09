import java.sql.*;

public class JDBCExample {
    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";// org.apache.hive.jdbc.HiveDriver为InceptorServer2的JDBC驱动，在和 InceptorServer2交互时使用。
    public static void main(String[] args) throws SQLException
    {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
//Hive2 JDBC URL with kerberos certification
//        String jdbcURL = "jdbc:inceptor2//InceptorserverIP:10000/default;principal=hive/
//        InceptorserverIP@TDH;"+"authentication=kerberos;'+"kuser=user_principal;"+"keytab=keytab_path;"+"krb5conf=krb5.conf_path";
//”jdbc:inceptor2://192.168.1.63:10000/default;principal=hive/tdh03@TDH;authentication=kerberos;kuser=hive;keytab=d:/kerberos/hive.keytab;krb5conf=d:/kerberos/krb5.conf”
        String jdbcURL = "jdbc:inceptor2://big-data-02:10000/;principal=hive/big-data-02@HADOOP.COM;" +
                "authentication=kerberos;kuser=hive;keytab=D:/work/gitlab/sparkcal/comm/src/main/resources/hive.keytab;krb5conf=C:/ProgramData/MIT/Kerberos5/krb5.ini";
        Connection conn = DriverManager.getConnection(jdbcURL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from ods_mysql.ods_lbn__rank_ful_p_day limit 1");
        while(rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getString(2));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
