package co.cask.hydrator.sqlcdc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test class to quickly run queries for debugging purpose. TODO: Remove this before final merge.
 */
public class Test {


  public static void main(String[] args) {
    try {
      Class.forName(SQLServerDriver.class.getName());
      String url = "jdbc:sqlserver://35.184.27.192:1433;DatabaseName=cttest";
      Connection conn = DriverManager.getConnection(url, "sa", "Realtime!23");
      getCT(conn, "ctone");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void getCT(Connection connection, String tableName) throws SQLException {
    Statement statement = connection.createStatement();
    String stmt = "SELECT * FROM CHANGETABLE (CHANGES ctone,0) as CT ORDER BY SYS_CHANGE_VERSION";
    ResultSet resultSet = statement.executeQuery(stmt);
    printResultSet(resultSet);

  }

  private static void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        if (i > 1) System.out.print(",  ");
        String columnValue = resultSet.getString(i);
        System.out.print(rsmd.getColumnName(i) + " " + columnValue);
      }
      System.out.println("");
    }
  }
}
