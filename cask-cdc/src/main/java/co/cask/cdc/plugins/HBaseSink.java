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

package co.cask.cdc.plugins;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCHBase")
@Description("Writes to Apache HBase tables.")
public class HBaseSink extends SparkCompute<StructuredRecord, StructuredRecord> {
  private static Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
  private final HBaseSinkConfig hBaseSinkConfig;

  public HBaseSink(HBaseSinkConfig config) {
    this.hBaseSinkConfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    if (hBaseSinkConfig.containsMacro("zookeeperQuorum") || hBaseSinkConfig.containsMacro("name")) {
      return;
    }
  }

  private void createHBaseTable(String tableName, Admin hBaseAdmin) {
    try {
      if(hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
        hBaseAdmin.createTable(new HTableDescriptor(TableName.valueOf(tableName)));
      } else {
        LOG.debug("HBase Table {} already exists.", tableName);
      }
    } catch (IOException ex) {
      LOG.error("Unable to create a HBase Table.", ex);
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    return javaRDD.mapPartitions(new FlatMapFunction<Iterator<StructuredRecord>, StructuredRecord>() {

      @Override
      public Iterable<StructuredRecord> call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {

        // initialize configuration
        String zkQuorum = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkQuorum()) ?
          hBaseSinkConfig.getZkQuorum() : "localhost";
        String zkClientPort = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkClientPort()) ?
          hBaseSinkConfig.getZkClientPort() : "2181";
        String zkNodeParent = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkNodeParent()) ?
          hBaseSinkConfig.getZkNodeParent() : "/hbase";
        Configuration transformConfig =HBaseConfiguration.create();
        transformConfig.addResource("hbase.zookeeper.quorum");
        transformConfig.set("hbase.zookeeper.quorum", String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));

        // create a connection to hbase
        Connection connection = ConnectionFactory.createConnection(transformConfig);
        Admin hBaseAdmin = connection.getAdmin();
        HBaseTableUpdater hBaseTableUpdater = new HBaseTableUpdater();

        while (structuredRecordIterator.hasNext()) {
          StructuredRecord input = structuredRecordIterator.next();
          String tableName = input.get("table").toString();
          LOG.info("Received StructuredRecord in HBase {}", GSON.toJson(input));
          LOG.info("StructuredRecord to StringConverter HBase {}", StructuredRecordStringConverter.toJsonString(input));
          if (input.getSchema().getRecordName().equals("DDLRecord")) {
            assert(input.getSchema().getField("table") != null);
            createHBaseTable(tableName, hBaseAdmin);
          } else {
            hBaseTableUpdater.updateHBaseTable(input, connection.getTable(TableName.valueOf(tableName)));
          }
        }
        hBaseAdmin.close();
        connection.close();

        return new Iterable<StructuredRecord>() {
          @Override
          public Iterator<StructuredRecord> iterator() {
            return new Iterator<StructuredRecord>() {
              @Override
              public boolean hasNext() {
                return false;
              }

              @Override
              public StructuredRecord next() {
                return null;
              }

              @Override
              public void remove() {
              }
            };
          }
        };
      }
    }, true);
  }

  private class HBaseTableUpdater {
    private final String columnFamily = "cdc";

    public void updateHBaseTable(StructuredRecord record, Table table) throws IOException{
      // a DML record
      List<String> primaryKeys = record.get("primary_keys");
      String opType = record.get("op_type");
      StructuredRecord change = record.get("change");
      String rowKey = "";
      for(String primaryKey : primaryKeys) {
        rowKey = rowKey.concat(change.get(primaryKey).toString());
      }

      // choose operation type
      switch (opType) {
        case "I":
        case "U":
          Put put;
          put = new Put(Bytes.toBytes(rowKey));
          for (Schema.Field field : change.getSchema().getFields()) {
            setPutField(put, columnFamily, field, change);
          }
          table.put(put);
          return;
        case "D":
          Delete delete;
          delete = new Delete(Bytes.toBytes(rowKey));
          for (Schema.Field field : change.getSchema().getFields()) {
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field.getName()));
          }
          table.delete(delete);
          return;
        default:
          throw new IllegalArgumentException(opType + "can only be \"I\" (insert), \"U\" (update), or \"D\" (delete)");
      }
    }

    private void setPutField(Put put, String family, Schema.Field field, StructuredRecord record) {
      // have to handle nulls differently. In a Put object, it's only valid to use the add(byte[], byte[])
      // for null values, as the other add methods take boolean vs Boolean, int vs Integer, etc.
      String query = field.getName();
      Object val = record.get(field.getName());
      if (field.getSchema().isNullable() && val == null) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), null);
        return;
      }

      Schema.Type type = validateAndGetType(field);

      switch (type) {
        case BOOLEAN:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Boolean) val));
          break;
        case INT:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Integer) val));
          break;
        case LONG:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Long) val));
          break;
        case FLOAT:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Float) val));
          break;
        case DOUBLE:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Double) val));
          break;
        case BYTES:
          if (val instanceof ByteBuffer) {
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((ByteBuffer) val));
          } else {
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), (byte[]) val);
          }
          break;
        case STRING:
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((String) val));
          break;
        default:
          throw new IllegalArgumentException("Field " + field.getName() + " is of unsupported type " + type);
      }
    }

    // get the non-nullable type of the field and check that it's a simple type.
    private Schema.Type validateAndGetType(Schema.Field field) {
      Schema.Type type;
      if (field.getSchema().isNullable()) {
        type = field.getSchema().getNonNullable().getType();
      } else {
        type = field.getSchema().getType();
      }
      Preconditions.checkArgument(type.isSimpleType(),
                                  "only simple types are supported (boolean, int, long, float, double, bytes).");
      return type;
    }
  }
}