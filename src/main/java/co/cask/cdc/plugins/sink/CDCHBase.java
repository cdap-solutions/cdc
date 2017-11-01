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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HBase sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCHBase")
public class CDCHBase extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCHBase.class);
  private final CDCHBaseConfig config;
  private final String CDC_COLUMN_FAMILY = "cdc";

  public CDCHBase(CDCHBaseConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception { }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    // Get the hadoop configurations and passed it as a Map to the closure
    Iterator<Map.Entry<String, String>> iterator = javaRDD.context().hadoopConfiguration().iterator();
    final Map<String, String> configs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      configs.put(next.getKey(), next.getValue());
    }

    // maps data sets to each block of computing resources
    javaRDD.foreachPartition(new VoidFunction<Iterator<StructuredRecord>>() {

      @Override
      public void call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {

        Job job;
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
        // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
        // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
        // points to).
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
          job = JobUtils.createInstance();
        } finally {
          // Switch back to the original
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

        Configuration conf = job.getConfiguration();

        for(Map.Entry<String, String> configEntry : configs.entrySet()) {
          conf.set(configEntry.getKey(), configEntry.getValue());
        }

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin hBaseAdmin = connection.getAdmin()) {
          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            LOG.debug("Received StructuredRecord in Kudu {}", StructuredRecordStringConverter.toJsonString(input));
            String tableName = getTableName((String) input.get("table"));
            if (input.getSchema().getRecordName().equals("DDLRecord")) {
              createHBaseTable(hBaseAdmin, tableName);
            } else {
              Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf(tableName));
              updateHBaseTable(table, input);
            }
          }
        }
      }
    });
  }


  private String getTableName(String namespacedTableName) {
    return namespacedTableName.split("\\.")[1];
  }

  private void createHBaseTable(Admin admin, String tableName) throws IOException {
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      descriptor.addFamily(new HColumnDescriptor(CDC_COLUMN_FAMILY));
      LOG.debug("Creating HBase table {}.", tableName);
      admin.createTable(descriptor);
    }
  }

  private byte[] getRowKey(List<String> primaryKeys, StructuredRecord change) {
    // the primary keys are always in sorted order
    List<String> primaryValues = new ArrayList<>();
    String [] primaryKeysArray = primaryKeys.toArray(new String[primaryKeys.size()]);
    Arrays.sort(primaryKeysArray);
    for(String primaryKey : primaryKeysArray) {
      primaryValues.add(change.get(primaryKey).toString());
    }
    String joinedValue = Joiner.on(":").join(primaryValues);
    return Bytes.toBytes(joinedValue);
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

  private void setPutField(Put put, Schema.Field field, @Nullable Object val) {
    Schema.Type type = validateAndGetType(field);
    String column = field.getName();
    if (val == null) {
      put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), null);
      return;
    }

    switch (type) {
      case BOOLEAN:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Boolean) val));
        break;
      case INT:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Integer) val));
        break;
      case LONG:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Long) val));
        break;
      case FLOAT:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Float) val));
        break;
      case DOUBLE:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Double) val));
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((ByteBuffer) val));
        } else {
          put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), (byte[]) val);
        }
        break;
      case STRING:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((String) val));
        break;
      default:
        throw new IllegalArgumentException("Field " + field.getName() + " is of unsupported type " + type);
    }
  }

  private void updateHBaseTable(Table table, StructuredRecord input) throws Exception {
    String operationType = input.get("op_type");
    List<String> primaryKeys = input.get("primary_keys");
    StructuredRecord change = input.get("change");
    List<Schema.Field> fields = change.getSchema().getFields();

    switch (operationType) {
      case "I":
      case "U":
        Put put = new Put(getRowKey(primaryKeys, change));
        for (Schema.Field field : fields) {
          setPutField(put, field, change.get(field.getName()));
        }
        table.put(put);
        LOG.info("XXX Putting row {}", Bytes.toString(getRowKey(primaryKeys, change)));
        break;
      case "D":
        Delete delete = new Delete(getRowKey(primaryKeys, change));
        table.delete(delete);
        LOG.info("XXX Deleting row {}", Bytes.toString(getRowKey(primaryKeys, change)));
        break;
      default:
        LOG.warn(String.format("Operation of type '%s' will be ignored.", operationType));
    }
  }

  public static class CDCHBaseConfig extends ReferencePluginConfig {
    public CDCHBaseConfig(String referenceName) {
      super(referenceName);
    }
  }
}

