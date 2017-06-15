/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdc.plugins.source.logminer;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdc.plugins.source.ReferenceStreamingSource;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import oracle.jdbc.driver.OracleDriver;
import org.apache.spark.api.java.JavaPairRDD;
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
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("OracleLogMiner")
@Description("Oracle LogMiner CDC Streaming Source")
public class OracleLogMiner extends ReferenceStreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(OracleLogMiner.class);
  private final ConnectionConfig conf;

  public OracleLogMiner(ConnectionConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    Connection connection = null;
    try {
      // https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides
      // Follow the instructions in the above link to find out how to include Oracle JDBC driver.
      Class.forName(OracleDriver.class.getName());
      if (conf.username != null && conf.password != null) {
        connection = DriverManager.getConnection(getConnectionString(), conf.username, conf.password);
      } else {
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }

      // get change information dtream. This dstream has both schema and data changes
      LOG.info("Creating change information dstream");
      ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);
      JavaDStream<StructuredRecord> changeDStream =
        JavaDStream.fromDStream(new ChangeInputDStream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                       getConnectionString(), conf.username, conf.password,
                                                       getTableNames(conf.tableNames), conf.startSCN), tag);


      JavaDStream<Long> count = changeDStream.count();
      System.out.println("The count is: " + count);

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
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private Set<String> getTableNames(String tableNames) {
    String[] split = tableNames.split(",");
    return Sets.newHashSet(split);
  }

  private String getConnectionString() {
    return String.format("jdbc:oracle:thin:@%s:%d:%s", conf.hostName, conf.port, conf.dbName);
  }

  private static class SchemaStateFunction implements Function3<String, Optional<StructuredRecord>,
    State<Map<String, String>>, Tuple2<StructuredRecord, Boolean>> {
    @Override
    public Tuple2<StructuredRecord, Boolean> call(String v1, Optional<StructuredRecord> value, State<Map<String, String>>
      stateStore) throws Exception {
      StructuredRecord input = value.get();
      // for dml record we don't need to maintain any state so skip it
      if (input.getSchema().getRecordName().equalsIgnoreCase(ResultSetToDMLRecord.RECORD_NAME)) {
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
