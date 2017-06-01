package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.plugin.DBUtils;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("SQLServerCDC")
@Description("SQL Server Change Data Capture Streaming Source")
public class SQLServerStreamingSource extends StreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerStreamingSource.class);

  private final ConnectionConfig conf;

  public enum CDCElement {
    DATABASE, TABLE
  }

  public SQLServerStreamingSource(ConnectionConfig conf) {
    this.conf = conf;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {

    Connection connection;
    try {
      Class.forName(SQLServerDriver.class.getName());
      if (conf.username != null && conf.password != null) {
        LOG.error("Creating connection with url {}, username {}, password *****", getConnectionString(), conf.username);
        connection = DriverManager.getConnection(getConnectionString(), conf.username, conf.password);
      } else {
        LOG.error("Creating connection with url {}", getConnectionString());
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    }

    // check that CDC is enabled on the database and the given table
    checkCTEnabled(connection, CDCElement.DATABASE, conf.dbName);
//    checkCTEnabled(connection, CDCElement.TABLE, conf.tableName);

    // get the capture instance detail of the the given table. We need this because this contains information about
    // the cdc table like its name and captured columns
//    CaptureInstanceDetail captureInstanceDetails = getCaptureInstanceDetails(connection, conf.tableName);
//    LOG.info("The captured instance details {} for table {}", captureInstanceDetails, conf.tableName);

    ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);

    List<TableInformation> ctEnabledTables = getCTEnabledTables(connection);

    JavaDStream<StructuredRecord> structuredRecordJavaDStream =
      JavaDStream.fromDStream(new CDCInputDStream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                  getConnectionString(), conf.username, conf
                                                    .password, ctEnabledTables), tag);

    structuredRecordJavaDStream.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord v1) throws Exception {
        System.out.println("### Printing lsn " + v1.get("__$start_lsn"));
        return v1;
      }
    });


    return structuredRecordJavaDStream;
  }

  private String getConnectionString() {
    return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", conf.hostname, conf.port,
                         conf.dbName);
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

  private List<TableInformation> getCTEnabledTables(Connection connection) throws SQLException {
    List<TableInformation> tableInformations = new LinkedList<>();
    String stmt = "SELECT s.name as schema_name, t.name AS table_name, ctt.* FROM sys.change_tracking_tables ctt " +
      "INNER JOIN sys.tables t on t.object_id = ctt.object_id INNER JOIN sys.schemas s on s.schema_id = t.schema_id";
    ResultSet rs = connection.createStatement().executeQuery(stmt);
    while (rs.next()) {
      String schemaName = rs.getString("schema_name");
      String tableName = rs.getString("table_name");
      tableInformations.add(new TableInformation(schemaName, tableName,
                                                 getColumnns(connection, schemaName, tableName),
                                                 getKeyColumns(connection, schemaName, tableName)));
    }
    return tableInformations;
  }

  private void checkCTEnabled(Connection connection, SQLServerStreamingSource.CDCElement type, String name)
    throws SQLException {
    // database
    String query = "SELECT * FROM sys.change_tracking_databases WHERE database_id=DB_ID(?)";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setString(1, name);
    ResultSet resultSet = preparedStatement.executeQuery();
    if (resultSet.next()) {
      // if resultset is not empty it means that our select with where clause returned data meaning ct is enabled.
      return;
    }
    throw new RuntimeException(String.format("Change Tracking is not enabled on the specified table '%s'. Please " +
                                               "enable it first.", name));
  }

}
