package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

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
 * A {@link InputDStream} which reads cdc data from SQL Server and emits {@link StructuredRecord}
 */
public class DDLInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerStreamingSource.class);

  private ClassTag<StructuredRecord> tag;
  private String connection;
  private String username;
  private String password;
  // transient to avoid serialization since SparkContext is not serializable
  private transient SparkContext sparkContext;
  private SQLServerConnection dbConnection;

  DDLInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password) {
    super(ssc, tag);
    this.tag = tag;
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    List<TableInformation> tableInformations = null;
    try {
      tableInformations = getCTEnabledTables(dbConnection.apply());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    List<RDD<StructuredRecord>> changeRDDs = new LinkedList<>();
    for (TableInformation tableInformation : tableInformations) {
      changeRDDs.add(getColumnns(tableInformation));
    }

    RDD<StructuredRecord> changes = sparkContext.union(JavaConversions.asScalaBuffer(changeRDDs), tag);
    return Option.apply(changes);
  }

  @Override
  public void start() {
    // create connection while start receiving data
    dbConnection = new SQLServerConnection(connection, username, password);
  }

  @Override
  public void stop() {
    // no-op
    // Also no need to close the dbconnection as JdbcRDD takes care of closing it
  }

  private JdbcRDD<StructuredRecord> getColumnns(TableInformation tableInformation) {

    final SparkContext sparkC = sparkContext;

    String stmt = String.format("SELECT TOP 1 * FROM [%s].[%s] where ?=?", tableInformation.getSchemaName(),
                                tableInformation.getName());

    return new JdbcRDD<>(sparkC, dbConnection, stmt, 1, 1, 1,
                         new ResultSetToDDLRecord(tableInformation.getSchemaName(), tableInformation.getName()),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
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
    connection.close();
    return tableInformations;
  }
}
