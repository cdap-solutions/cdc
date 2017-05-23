package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * A {@link InputDStream} which reads cdc data from SQL Server and emits {@link StructuredRecord}
 */
public class CDCInputDStream extends InputDStream<StructuredRecord> {
  private String connection;
  private String username;
  private String password;
  private SQLServerStreamingSource.CaptureInstanceDetail captureInstanceDetail;
  // transient to avoid serialization since SparkContext is not serializable
  private transient SparkContext sparkContext;
  private SQLServerConnection dbConnection;

  CDCInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                  String password, SQLServerStreamingSource.CaptureInstanceDetail captureInstanceDetail) {
    super(ssc, tag);
    this.sparkContext = ssc.sparkContext();
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.captureInstanceDetail = captureInstanceDetail;
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

    String stmt = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + captureInstanceDetail.captureInstanceName + "(sys.fn_cdc_get_min_lsn('" +
      captureInstanceDetail.captureInstanceName + "'), sys.fn_cdc_get_max_lsn(), 'all') WHERE ? = ?";

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(sparkC, dbConnection, stmt, 1, 1, 1, new ResultSetToStructureRecord(),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
  }
}
