package co.cask.cdc.plugins.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rsinha on 6/15/17.
 */
public class SchemaState implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(co.cask.cdc.plugins.common.SchemaState.class);

  public static JavaDStream<StructuredRecord> filter(JavaDStream<StructuredRecord>
                                                        changeDStream) {
    JavaPairDStream<String, StructuredRecord> pairedChangeDStream =
      changeDStream.mapToPair(new PairFunction<StructuredRecord, String, StructuredRecord>() {
        @Override
        public Tuple2<String, StructuredRecord> call(StructuredRecord structuredRecord) throws Exception {
          return new Tuple2<>("", structuredRecord);
        }
      });


    // map the dstream with schema state store to detect changes in schema
    JavaMapWithStateDStream<String, StructuredRecord, Map<String, String>,
      Tuple2<StructuredRecord, Boolean>> stateMappedChangeDStream =
      pairedChangeDStream.mapWithState(StateSpec.function(new SchemaStateFunction()));

    // filter out the ddl record whose schema hasn't changed and then drop all the keys
    JavaDStream<StructuredRecord> map = stateMappedChangeDStream.filter(new Function<Tuple2<StructuredRecord, Boolean>, Boolean>() {
      @Override
      public Boolean call(Tuple2<StructuredRecord, Boolean> v1) throws Exception {
        return v1._2();
      }
    }).map(new Function<Tuple2<StructuredRecord, Boolean>, StructuredRecord>() {
      @Override
      public StructuredRecord call(Tuple2<StructuredRecord, Boolean> v1) throws Exception {
        return v1._1();
      }
    });

    return map.mapToPair(new PairFunction<StructuredRecord, String, StructuredRecord>() {
      @Override
      public Tuple2<String, StructuredRecord> call(StructuredRecord structuredRecord) throws Exception {
        // key by record name DDLRecord or DMLRecord and the record
        return new Tuple2<>(structuredRecord.getSchema().getRecordName(), structuredRecord);
      }
    }).transformToPair(new Function<JavaPairRDD<String, StructuredRecord>,
      JavaPairRDD<String, StructuredRecord>>() {
      @Override
      public JavaPairRDD<String, StructuredRecord> call(JavaPairRDD<String, StructuredRecord> v1) throws Exception {
        // sort by key so that all DDLRecord comes first
        return v1.sortByKey();
      }
    }).map(new Function<Tuple2<String,
      StructuredRecord>, StructuredRecord>() {
      @Override
      public StructuredRecord call(Tuple2<String, StructuredRecord> v1) throws Exception {
        // drop the keys
        return v1._2();
      }
    });
  }

  public static class SchemaStateFunction implements Function3<String, Optional<StructuredRecord>,

      State<Map<String, String>>, Tuple2<StructuredRecord, Boolean>> {

    @Override
    public Tuple2<StructuredRecord, Boolean> call(String v1, Optional<StructuredRecord> value, State<Map<String, String>>
      stateStore) throws Exception {
      StructuredRecord input = value.get();
      // for dml record we don't need to maintain any state so skip it
      if (input.getSchema().getRecordName().equalsIgnoreCase(Constants.DMLRecord.RECORD_NAME)) {
        return new Tuple2<>(value.get(), true);
      }

      // we know now that its a ddl record so process it
      String tableName = input.get("table");
      String tableSchemaStructure = input.get("schema");
      Map<String, String> state;
      if (stateStore.exists()) {
        state = stateStore.get();
        if (state.containsKey(tableName) && state.get(tableName).equals(tableSchemaStructure)) {
          // schema hasn't changed so emit with false so that we can later filter this record out
          return new Tuple2<>(value.get(), false);
        }
      } else {
        state = new HashMap<>();
      }
      // update the state
      state.put(tableName, tableSchemaStructure);
      stateStore.update(state);
      LOG.info("Update schema state store for table {}. New schema will be emitted.", tableName);
      return new Tuple2<>(value.get(), true);
    }
  }
}
