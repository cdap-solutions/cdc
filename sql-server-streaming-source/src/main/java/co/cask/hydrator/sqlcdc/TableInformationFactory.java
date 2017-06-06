package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Throwables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by rsinha on 6/5/17.
 */
public class TableInformationFactory {
  private final SQLServerConnection dbConnection;

  public TableInformationFactory(SQLServerConnection dbConnection) {
    this.dbConnection = dbConnection;
  }

  private List<Schema.Field> getColumnns(Connection connection, String schema, String table) throws SQLException {
    String query = String.format("SELECT * from [%s].[%s]", schema, table);
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    ResultSet resultSet = statement.executeQuery(query);
    return DBUtils.getSchemaFields(resultSet);
  }

  private Set<String> getKeyColumns(Connection connection, String schema, String table) throws SQLException {
    String stmt =
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " +
        "OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND " +
        "TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    Set<String> keyColumns = new LinkedHashSet<>();
    try (PreparedStatement primaryKeyStatement = connection.prepareStatement(stmt)) {
      primaryKeyStatement.setString(1, schema);
      primaryKeyStatement.setString(2, table);
      try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
        while (resultSet.next()) {
          keyColumns.add(resultSet.getString(1));
        }
      }
    }
    return keyColumns;
  }

  List<TableInformation> getCTEnabledTables() {
    Connection connection = dbConnection.apply();
    List<TableInformation> tableInformations = new LinkedList<>();
    String stmt = "SELECT s.name as schema_name, t.name AS table_name, ctt.* FROM sys.change_tracking_tables ctt " +
      "INNER JOIN sys.tables t on t.object_id = ctt.object_id INNER JOIN sys.schemas s on s.schema_id = t.schema_id";
    try {
      ResultSet rs = connection.createStatement().executeQuery(stmt);
      while (rs.next()) {
        String schemaName = rs.getString("schema_name");
        String tableName = rs.getString("table_name");
        tableInformations.add(new TableInformation(schemaName, tableName,
                                                   getColumnns(connection, schemaName, tableName),
                                                   getKeyColumns(connection, schemaName, tableName)));
      }
      connection.close();
      return tableInformations;
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }
}
