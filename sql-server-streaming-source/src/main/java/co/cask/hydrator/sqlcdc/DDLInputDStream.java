package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
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

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link InputDStream} which reads cdc data from SQL Server and emits {@link StructuredRecord}
 */
public class DDLInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerStreamingSource.class);

  private ClassTag<StructuredRecord> tag;
  private String connection;
  private String username;
  private String password;
  private List<TableInformation> tableInformations;
  // transient to avoid serialization since SparkContext is not serializable
  private transient SparkContext sparkContext;
  private SQLServerConnection dbConnection;

  DDLInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password, List<TableInformation> tableInformations) {
    super(ssc, tag);
    this.tag = tag;
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.tableInformations = tableInformations;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
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
}
