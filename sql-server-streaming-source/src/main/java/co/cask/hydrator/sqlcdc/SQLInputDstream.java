package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;
import scala.reflect.ClassTag;

/**
 * A simple InputDStream which just wraps around a given rdd to create a DStream
 */
public class SQLInputDstream extends InputDStream<StructuredRecord> {

  RDD<StructuredRecord> rdd;

  SQLInputDstream(StreamingContext ssc, ClassTag<StructuredRecord> evidence$1, RDD<StructuredRecord> rdd) {
    super(ssc, evidence$1);
    this.rdd = rdd;

  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    return Option.apply(this.rdd);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }
}
