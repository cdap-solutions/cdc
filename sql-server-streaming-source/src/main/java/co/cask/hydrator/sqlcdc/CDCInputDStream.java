package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.base.Joiner;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A {@link InputDStream} which reads cdc data from SQL Server and emits {@link StructuredRecord}
 */
public class CDCInputDStream extends InputDStream<StructuredRecord> {
  private String connection;
  private String username;
  private String password;
  private TableInformation tableInformation;
  // transient to avoid serialization since SparkContext is not serializable
  private transient SparkContext sparkContext;
  private SQLServerConnection dbConnection;

  CDCInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password, TableInformation tableInformation) {
    super(ssc, tag);
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.tableInformation = tableInformation;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    return Option.apply(getChangeData());
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

  private RDD<StructuredRecord> getChangeData() {

    final SparkContext sparkC = sparkContext;

    String stmt = String.format("SELECT [CT].[SYS_CHANGE_VERSION], [CT].[SYS_CHANGE_CREATION_VERSION], " +
                                  "[CT].[SYS_CHANGE_OPERATION], %s, %s FROM [%s] as [CI] RIGHT OUTER JOIN " +
                                  "CHANGETABLE (CHANGES [%s], %s) as [CT] on %s WHERE ? = ? ORDER BY [CT]" +
                                  ".[SYS_CHANGE_VERSION]",
                                joinSelect("CT", tableInformation.getPrimaryKeys()),
                                joinSelect("CI", tableInformation.getValueColumnNames()),
                                tableInformation.getName(), tableInformation.getName(), 0, joinCriteria
                                  (tableInformation.getPrimaryKeys()));

    System.out.println("Query String: " + stmt);
    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(sparkC, dbConnection, stmt, 1, 1, 1, new ResultSetToStructureRecord(),
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
}
