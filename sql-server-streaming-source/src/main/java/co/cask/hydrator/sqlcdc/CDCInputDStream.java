package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.base.Joiner;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A {@link InputDStream} which reads cdc data from SQL Server and emits {@link StructuredRecord}
 */
public class CDCInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerStreamingSource.class);
  private ClassTag<StructuredRecord> tag;
  private String connection;
  private String username;
  private String password;
  private List<TableInformation> tableInformations;
  // transient to avoid serialization since SparkContext is not serializable
  private transient SparkContext sparkContext;
  private SQLServerConnection dbConnection;
  private long currentTrackingVersion;

  CDCInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password, List<TableInformation> tableInformations, long currentTrackingVersion) {
    super(ssc, tag);
    this.tag = tag;
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.tableInformations = tableInformations;
    this.currentTrackingVersion = currentTrackingVersion;
  }

  CDCInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password, List<TableInformation> tableInformations) {
    super(ssc, tag);
    this.tag = tag;
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.tableInformations = tableInformations;
    this.currentTrackingVersion = 0;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    long prev = currentTrackingVersion;
    try {
      currentTrackingVersion = getCurrentTrackingVersion(dbConnection.apply());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    List<RDD<StructuredRecord>> changeRDDs = new LinkedList<>();
    for (TableInformation tableInformation : tableInformations) {
      changeRDDs.add(getChangeData(tableInformation, prev, currentTrackingVersion));
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

  private RDD<StructuredRecord> getChangeData(TableInformation tableInformation, long prev, long currentTrackingVersion) {

    final SparkContext sparkC = sparkContext;

    String stmt = String.format("SELECT [CT].[SYS_CHANGE_VERSION], [CT].[SYS_CHANGE_CREATION_VERSION], " +
                                  "[CT].[SYS_CHANGE_OPERATION], %s, %s FROM [%s] as [CI] RIGHT OUTER JOIN " +
                                  "CHANGETABLE (CHANGES [%s], %s) as [CT] on %s where [CT]" +
                                  ".[SYS_CHANGE_CREATION_VERSION] > ? " +
                                  "and [CT].[SYS_CHANGE_CREATION_VERSION] <= ? ORDER BY [CT]" +
                                  ".[SYS_CHANGE_VERSION]",
                                joinSelect("CT", tableInformation.getPrimaryKeys()),
                                joinSelect("CI", tableInformation.getValueColumnNames()),
                                tableInformation.getName(), tableInformation.getName(), 0, joinCriteria
                                  (tableInformation.getPrimaryKeys()));

    LOG.info("Query String: {}" + stmt);
    LOG.info("### the prev {} curr {}", prev, currentTrackingVersion);
    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(sparkC, dbConnection, stmt, prev, this.currentTrackingVersion, 1,
                         new ResultSetToStructureRecord(tableInformation.getSchemaName(), tableInformation.getName()),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
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

  private String joinSelect(String table, Collection<String> keyColumns) {
    List<String> selectColumns = new ArrayList<>(keyColumns.size());

    for (String keyColumn : keyColumns) {
      selectColumns.add(String.format("[%s].[%s]", table, keyColumn));
    }

    return Joiner.on(", ").join(selectColumns);
  }

  private long getCurrentTrackingVersion(Connection connection) throws SQLException {
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
    long changeVersion = 0;
    while(resultSet.next()) {
      changeVersion = resultSet.getLong(1);
    }
    connection.close();
    return changeVersion;
  }
}
