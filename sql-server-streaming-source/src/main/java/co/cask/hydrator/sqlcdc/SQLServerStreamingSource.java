package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

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
    checkCDCEnabled(connection, CDCElement.TABLE, conf.tableName);

    // get the capture instance detail of the the given table. We need this because this contains information about
    // the cdc table like its name and captured columns
    CaptureInstanceDetail captureInstanceDetails = getCaptureInstanceDetails(connection, conf.tableName);
    LOG.info("The captured instance details {} for table {}", captureInstanceDetails, conf.tableName);

    ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);

    return JavaDStream.fromDStream(new CDCInputDStream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                       getConnectionString(), conf.username, conf.password,
                                                       captureInstanceDetails), tag);
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

  public class CaptureInstanceDetail implements Serializable {
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


  private String getPrimaryKeyName(Connection connection, String tableName) throws SQLException {
    //TODO: this will not be true in all cases. It is possible to enable cdc on a table without primary key in which
    // an unique index key can be specified. Change this to support the later case
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("sp_pkeys " + tableName);
    resultSet.next();
    return resultSet.getString("COLUMN_NAME");
  }
}
