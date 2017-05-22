package co.cask.hydrator.sqlcdc;

import com.google.common.io.ByteStreams;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Test class to quickly run queries for debugging purpose. TODO: Remove this before final merge.
 */
public class Test {


  public static void main(String[] args) {
    try {
      Class.forName(SQLServerDriver.class.getName());
      String url = "jdbc:sqlserver://35.184.27.192:1433;DatabaseName=cdcttwo";
      Connection conn = DriverManager.getConnection(url, "sa", "Realtime!23");
      String tone = getPrimaryKeyName(conn, "tone");
      System.out.printf("pk: " + tone);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static String getPrimaryKeyName(Connection connection, String tableName) throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("sp_pkeys " + tableName);
    resultSet.next();
    return resultSet.getString("COLUMN_NAME");
  }

  private static void showCDC(Connection connection, String name) throws SQLException, IOException {
    Statement statement = connection.createStatement();
    String queryString = "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_testtable(sys.fn_cdc_get_min_lsn('dbo_testtable'), sys.fn_cdc_get_max_lsn(), 'all') ORDER BY __$seqval";
    ResultSet rs = statement.executeQuery(queryString);
    while (rs.next()) {
      System.out.println(Arrays.toString(ByteStreams.toByteArray(rs.getBinaryStream(1))));
    }
  }
}
