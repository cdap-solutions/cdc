package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Throwables;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("SQLServerCDC")
@Description("SQL Server Change Data Capture Streaming Source")
public class SQLServerStreamingSource extends StreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerStreamingSource.class);

  private final ConnectionConfig conf;

  private enum CDCElement {
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
    checkCDCEnabled(connection, CDCElement.DATABASE, conf.dbName);
    checkCDCEnabled(connection, CDCElement.DATABASE, conf.dbName);

    // get the capture instance detail of the the given table. We need this because this contains information about
    // the cdc table like its name and captured columns
    CaptureInstanceDetail captureInstanceDetails = getCaptureInstanceDetails(connection, conf.tableName);
    LOG.info("The captured instance details {} for table {}", captureInstanceDetails, conf.tableName);

    JavaRDD<StructuredRecord> rdd = getChangeData(streamingContext, captureInstanceDetails);
    ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);

    return JavaDStream.fromDStream(new SQLInputDstream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                       rdd.rdd()), tag);
  }

  private String getConnectionString() {
    return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", conf.hostname, conf.port,
                         conf.dbName);
  }

  private static void checkCDCEnabled(Connection conn, CDCElement element, String name) throws SQLException {

    Statement statement = conn.createStatement();
    String queryString;
    if (element == CDCElement.DATABASE) {
      queryString = "SELECT name FROM sys.databases WHERE is_cdc_enabled = 1 AND name = '" + name + "'";
    } else if (element == CDCElement.TABLE) {
      queryString = "SELECT name FROM sys.tables WHERE is_tracked_by_cdc = 1 AND name = '" + name + "'";
    } else {
      throw new IllegalArgumentException(String.format("Failed to check CDC enable status on unsupported type '%s' " +
                                                         "named '%s'", element, name));
    }

    if (!statement.executeQuery(queryString).next()) {
      throw new RuntimeException(String.format("CDC is not enabled on %s '%s'. Please enable it and try deploying the" +
                                                 " pipeline again.", element, name));
    }
  }

  class CaptureInstanceDetail implements Serializable {
    final String captureInstanceName; // the change table name
    final boolean supportNetChanges; // whether the cdc is enabled to support net changes
    final List<String> indexColumnList; // name of index columns of the table being tracked
    final List<String> capturedColumnList; // name of all the captured columns of the table being tracked
    final int objectId; // an unique identifier for the change table
    final String primaryKeyName;

    CaptureInstanceDetail(String captureInstanceName, boolean supportNetChanges, String indexColumnList,
                          String capturedColumnList, int objectId, String primaryKeyName) {
      this.captureInstanceName = captureInstanceName;
      this.supportNetChanges = supportNetChanges;
      this.indexColumnList = getList(indexColumnList);
      this.capturedColumnList = getList(capturedColumnList);
      this.objectId = objectId;
      this.primaryKeyName = primaryKeyName;
    }

    @Override
    public String toString() {
      return "CaptureInstanceDetail{" +
        "captureInstanceName='" + captureInstanceName + '\'' +
        ", supportNetChanges=" + supportNetChanges +
        ", indexColumnList=" + indexColumnList +
        ", capturedColumnList=" + capturedColumnList +
        ", objectId=" + objectId +
        ", primaryKeyName='" + primaryKeyName + '\'' +
        '}';
    }

    private List<String> getList(String s) {
      return Arrays.asList(s.split("\\s*,\\s*"));
    }
  }

  private CaptureInstanceDetail getCaptureInstanceDetails(Connection connection, String name) throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("EXEC sys.sp_cdc_help_change_data_capture");
    String primaryKeyName = getPrimaryKeyName(connection, name);

    while (rs.next()) {
      if (rs.getString("source_table").equalsIgnoreCase(name)) {
        return new CaptureInstanceDetail(rs.getString("capture_instance"), rs.getBoolean("supports_net_changes"), rs
          .getString("index_column_list"), rs.getString("captured_column_list"), rs.getInt("object_id"), primaryKeyName);
      }
    }
    throw new RuntimeException(String.format("Capture instance not found for table %s", name));
  }

  private JavaRDD<StructuredRecord> getChangeData(StreamingContext streamingContext, CaptureInstanceDetail captureInstanceDetail) {

    SQLServerConnection dbConnection = new SQLServerConnection(getConnectionString(), conf.username, conf.password);
    String stmt = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + captureInstanceDetail.captureInstanceName + "(sys.fn_cdc_get_min_lsn('" +
      captureInstanceDetail.captureInstanceName + "'), sys.fn_cdc_get_max_lsn(), 'all') WHERE ? = ?";

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    JdbcRDD<StructuredRecord> jdbcRDD =
      new JdbcRDD<>(streamingContext.getSparkStreamingContext().sparkContext().sc(), dbConnection, stmt, 1,
                    1, 1, new MapResult(captureInstanceDetail), ClassManifestFactory$.MODULE$.fromClass
        (StructuredRecord.class));

    return JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
  }

  private String getPrimaryKeyName(Connection connection, String tableName) throws SQLException {
    //TODO: this will not be true in all cases. It is possible to enable cdc on a table without primary key in which
    // an unique index key can be specified. Change this to support the later case
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("sp_pkeys " + tableName);
    resultSet.next();
    return resultSet.getString("COLUMN_NAME");
  }

  static class MapResult extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {
    final CaptureInstanceDetail captureInstanceDetail;

    MapResult(CaptureInstanceDetail captureInstanceDetail) {
      this.captureInstanceDetail = captureInstanceDetail;
    }

    public StructuredRecord apply(ResultSet row) {
      try {
        return resultSetToStructureRecord(row, captureInstanceDetail.primaryKeyName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static StructuredRecord resultSetToStructureRecord(ResultSet resultSet, String primaryKeyName) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(resultSet);
    schemaFields.add(Schema.Field.of("primaryKey", Schema.of(Schema.Type
                                                               .STRING)));
    Schema schema = Schema.recordOf("dbRecord", schemaFields);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (int i = 0; i < schemaFields.size() - 1; i++) {
      Schema.Field field = schemaFields.get(i);
      int sqlColumnType = metadata.getColumnType(i + 1);
      recordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet, field.getName()));
    }
    recordBuilder.set("primaryKey", primaryKeyName);
    return recordBuilder.build();
  }

  //TODO: This function is taken from DatabaseSource. We should move it to the DBUtil class in Datasbase plugin and
  // use it here.
  @Nullable
  private static Object transformValue(int sqlColumnType, ResultSet resultSet, String fieldName) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlColumnType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          return ((BigDecimal) original).doubleValue();
        case Types.DATE:
          return resultSet.getDate(fieldName).getTime();
        case Types.TIME:
          return resultSet.getTime(fieldName).getTime();
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(fieldName).getTime();
        case Types.BLOB:
          Object toReturn;
          Blob blob = (Blob) original;
          try {
            toReturn = blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
          return toReturn;
        case Types.CLOB:
          String s;
          StringBuilder sbf = new StringBuilder();
          Clob clob = (Clob) original;
          try {
            try (BufferedReader br = new BufferedReader(clob.getCharacterStream(1, (int) clob.length()))) {
              if ((s = br.readLine()) != null) {
                sbf.append(s);
              }
              while ((s = br.readLine()) != null) {
                sbf.append(System.getProperty("line.separator"));
                sbf.append(s);
              }
            }
          } catch (IOException e) {
            throw new SQLException(e);
          } finally {
            clob.free();
          }
          return sbf.toString();
      }
    }
    return original;
  }

  public class SQLServerConnection extends AbstractFunction0<Connection> implements Serializable {
    private String connectionUrl;
    private String userName;
    private String password;

    SQLServerConnection(String connectionUrl, String userName, String password) {
      this.connectionUrl = connectionUrl;
      this.userName = userName;
      this.password = password;
    }

    @Override
    public Connection apply() {
      try {
        Class.forName(SQLServerDriver.class.getName());
        Properties properties = new Properties();
        properties.setProperty("user", userName);
        properties.setProperty("password", password);
        return DriverManager.getConnection(connectionUrl, properties);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
