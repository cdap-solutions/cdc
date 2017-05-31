package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Test class to quickly run queries for debugging purpose. TODO: Remove this before final merge.
 */
public class Test {


  public static void main(String[] args) {
    try {
      Class.forName(SQLServerDriver.class.getName());
      String url = "jdbc:sqlserver://35.184.27.192:1433;DatabaseName=ctest";
      Connection connection = DriverManager.getConnection(url, "sa", "Realtime!23");
      String tableName = "ctone";
      System.out.println(getCurrentTrackingVersion(connection));

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

  private static ResultSet getCTEnabledTables(Connection connection) throws SQLException {
    String stmt = "SELECT s.name as Schema_name, t.name AS Table_name, tr.* FROM sys.change_tracking_tables tr " +
      "INNER JOIN sys.tables t on t.object_id = tr.object_id INNER JOIN sys.schemas s on s.schema_id = t.schema_id";
    return connection.createStatement().executeQuery(stmt);
  }


  private static String joinCriteria(Set<String> keyColumns) {
    StringBuilder joinCriteria = new StringBuilder();
    for (String keyColumn : keyColumns) {
      if (joinCriteria.length() > 0) {
        joinCriteria.append(" AND ");
      }

      joinCriteria.append(
        String.format("[CT].[%s] = [CI].[%s]", keyColumn, keyColumn)
      );
    }
    return joinCriteria.toString();
  }


  private static long getCurrentTrackingVersion(Connection connection) throws SQLException {
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
    long changeVersion = 0;
    while(resultSet.next()) {
      changeVersion = resultSet.getLong(1);
    }
    return changeVersion;
  }
}
