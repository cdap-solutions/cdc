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
      String url = "jdbc:sqlserver://35.184.27.192:1433;DatabaseName=cttest";
      Connection connection = DriverManager.getConnection(url, "sa", "Realtime!23");
      String tableName = "ctone";
//      checkCTEnabled(connection, SQLServerStreamingSource.CDCElement.DATABASE, "cttest");
//      checkCTEnabled(connection, SQLServerStreamingSource.CDCElement.TABLE, tableName);
      ResultSet id = getChanges(connection, tableName, 0, Sets.newHashSet("ID"));
//      System.out.println(getKeyColumns(connection));
//      getSchema(connection);
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
    ResultSetToStructureRecord resultSetToStructureRecord = new ResultSetToStructureRecord();
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

  private static void checkCTEnabled(Connection connection, SQLServerStreamingSource.CDCElement type, String name)
    throws SQLException {
    if (type == SQLServerStreamingSource.CDCElement.TABLE) {
      ResultSet ctEnabledTables = getCTEnabledTables(connection);
      while (ctEnabledTables.next()) {
        if (ctEnabledTables.getString("Table_name").equalsIgnoreCase(name)) {
          return;
        }
      }
    } else {
      // database
      String query = "SELECT * FROM sys.change_tracking_databases WHERE database_id=DB_ID(?)";
      PreparedStatement preparedStatement = connection.prepareStatement(query);
      preparedStatement.setString(1, name);
      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        // if resultset is not empty it means that our select with where clause returned data meaning ct is enabled.
        return;
      }
    }
    throw new RuntimeException(String.format("Change Tracking is not enabled on the specified table '%s'. Please " +
                                               "enable it first.", name));
  }

  private static ResultSet getChanges(Connection connection, String tableName, long changeIndex,
                                      Set<String> primaryKeys)
    throws SQLException {
//
//    String stmt = String.format("SELECT CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_CREATION_VERSION, " +
//                                  "CT.SYS_CHANGE_OPERATION, CI.* FROM %s as CI RIGHT OUTER JOIN CHANGETABLE (CHANGES" +
//                                  " " +
//                                  "%s,%s) as CT on %s ORDER BY SYS_CHANGE_VERSION",
//                                tableName, tableName, changeIndex, joinCriteria(primaryKeys));
    String stmt = String.format("SELECT [CT].[SYS_CHANGE_VERSION], [CT].[SYS_CHANGE_CREATION_VERSION], " +
                                  "[CT].[SYS_CHANGE_OPERATION], %s, %s FROM [%s] as [CI] RIGHT OUTER JOIN " +
                                  "CHANGETABLE " +
                                  "(CHANGES" +
                                  " " +
                                  "[%s],%s) as [CT] on %s ORDER BY [CT].[SYS_CHANGE_VERSION]",
                                joinSelect("CT", Sets.newHashSet("ID", "MY")), joinSelect("CI", Sets.newHashSet("NAME",
                                                                                                                "DEPT"))
      , tableName, tableName,
                                changeIndex,
                                joinCriteria
                                  (primaryKeys));
//    ResultSet resultSet = connection.createStatement().executeQuery(stmt);
//    printResultSet(resultSet);
//    return resultSet;
    System.out.println(stmt);
    return null;
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

  public static Set<String> getKeyColumns(Connection connection) throws SQLException {
    String stmt =
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " +
        "OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND " +
        "TABLE_NAME = ?";
    Set<String> keyColumns = new LinkedHashSet<>();
    try (PreparedStatement primaryKeyStatement = connection.prepareStatement(stmt)) {
      primaryKeyStatement.setString(1, "ctone");
      try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
        while (resultSet.next()) {
          keyColumns.add(resultSet.getString(1));
        }
      }
    }
    return keyColumns;
  }

  private static String joinSelect(String table, Collection<String> keyColumns) {
    List<String> selectColumns = new ArrayList<>(keyColumns.size());

    for (String keyColumn : keyColumns) {
      selectColumns.add(String.format("[%s].[%s]", table, keyColumn));
    }

    return Joiner.on(", ").join(selectColumns);
  }

  private static void getSchema(Connection connection) throws SQLException {
    String query = String.format("SELECT * from %s", "ctone");
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    ResultSet resultSet = statement.executeQuery(query);
    Schema tableSchema = Schema.recordOf("tableSchema", DBUtils.getSchemaFields(resultSet));
    System.out.printf(tableSchema.toString());
  }
}
