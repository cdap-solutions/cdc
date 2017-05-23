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
 * A simple InputDStream which just wraps around a given rdd to create a DStream
 */
public class SQLInputDstream extends InputDStream<StructuredRecord> {

  private String connection;
  private String username;
  private String password;
  private SQLServerStreamingSource.CaptureInstanceDetail captureInstanceDetail;
  private transient SparkContext sparkContext; // transient to avoid serialization since SparkContext is not serializable

  SQLInputDstream(StreamingContext ssc, ClassTag<StructuredRecord> evidence$1, String connection,
                  String username, String password, SQLServerStreamingSource.CaptureInstanceDetail captureInstanceDetail) {
    super(ssc, evidence$1);
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
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }

  private RDD<StructuredRecord> getChangeData() {
    final SparkContext sparkC = sparkContext;

    SQLServerConnection dbConnection = new SQLServerConnection(connection, username, password);
    String stmt = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + captureInstanceDetail.captureInstanceName + "(sys.fn_cdc_get_min_lsn('" +
      captureInstanceDetail.captureInstanceName + "'), sys.fn_cdc_get_max_lsn(), 'all') WHERE ? = ?";

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(sparkC, dbConnection, stmt, 1, 1, 1, new ResultSetToStructureRecord(),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));


//    return JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(ResultSet.class));
  }
}
