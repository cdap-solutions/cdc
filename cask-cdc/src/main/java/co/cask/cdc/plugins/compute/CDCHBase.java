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

package co.cask.cdc.plugins.compute;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HBase sink for CDC
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("CDCHBase")
public class CDCHBase extends SparkCompute<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCHBase.class);
  private static Gson GSON = new Gson();
  private final CDCHBaseConfig config;
  private final String CDC_COLUMN_FAMILY = "cdc";

  public CDCHBase(CDCHBaseConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    // get local node configurations and pass them as a Map
    Iterator<Map.Entry<String, String>> iterator = javaRDD.context().hadoopConfiguration().iterator();
    final Map<String, String> configs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      configs.put(next.getKey(), next.getValue());
    }
    LOG.info("HBase returning from here");
    return javaRDD.mapPartitions(new FlatMapFunction<Iterator<StructuredRecord>, StructuredRecord>() {

      @Override
      public Iterable<StructuredRecord> call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {
        LOG.info("HBASEXXX");

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

        // initialize the zookeeper quorum settings
        String zkQuorum = !Strings.isNullOrEmpty(config.zkQuorum) ? config.zkQuorum : "localhost";
        String zkClientPort = !Strings.isNullOrEmpty(config.zkClientPort) ? config.zkClientPort : "2181";
        String zkNodeParent = !Strings.isNullOrEmpty(config.zkNodeParent) ? config.zkNodeParent : "/hbase";
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zkClientPort);
        conf.set("zookeeper.znode.parent", zkNodeParent);
        LOG.info("Zookeeper quorum to HBASEXXX {}", String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));

        // now we put the node configurations in conf
        Iterator<Map.Entry<String,String>> configIterator = configs.entrySet().iterator();
        while (configIterator.hasNext()) {
          Map.Entry<String,String> nextConfig = configIterator.next();
          conf.set(nextConfig.getKey(), nextConfig.getValue());
        }

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin hBaseAdmin = connection.getAdmin()) {
          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            LOG.info("Received StructuredRecord in HBase {}", GSON.toJson(input));
            LOG.info("StructuredRecord to StringConverter HBase {}", StructuredRecordStringConverter.toJsonString(input));
            if (input.getSchema().getRecordName().equals("DDLRecord")) {
              createHBaseTable(hBaseAdmin, (String) input.get("table"));
            } else {
              Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf((String) input.get("table")));
              updateHBaseTable(table, input);
            }

          }
        } catch (Throwable t) {
          LOG.error("Exception  ", t);
        }

        LOG.info("HBASEXXX After exception {}");

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


  private void createHBaseTable(Admin admin, String tableName) throws IOException {
    LOG.info("Creating HBase table {}.", tableName);
    try {
      if (!admin.tableExists(TableName.valueOf(tableName))) {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        descriptor.addFamily(new HColumnDescriptor(CDC_COLUMN_FAMILY));
        admin.createTable(descriptor);
      }
    } catch (Throwable t) {
      LOG.error("exception occurred.", t);
    }
  }

  private byte[] getRowKey(List<String> primaryKeys, StructuredRecord change) {
    // TODO make sure the primary keys are always in same order
    List<String> primaryValues = new ArrayList<>();
    for(String primaryKey : primaryKeys) {
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

  private void setPutField(Put put, Schema.Field field, Object val) {
    Schema.Type type = validateAndGetType(field);
    String column = field.getName();
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
    List<Schema.Field> fields = Schema.parseJson((String)input.get("schema")).getFields();
    LOG.info("Operation type is {}", operationType);
    for(String pKey: primaryKeys) {
      LOG.info("prim key : {}", pKey);
    }

    switch (operationType) {
      case "I":
      case "U":
        Put put = new Put(getRowKey(primaryKeys, change));
        for (Schema.Field field : fields) {
          // Normalizer always passes the full schema, however it is possible that only few fields are provided
          // Check if the field is actually provided
          if (change.get(field.getName()) != null) {
            setPutField(put, field, change.get(field.getName()));
          }
        }
        table.put(put);
        break;
      case "D":
        Delete delete = new Delete(getRowKey(primaryKeys, change));
        table.delete(delete);
        break;
      default:
        LOG.warn(String.format("Operation of type '%s' will be ignored.", operationType));
    }
  }

  public static class CDCHBaseConfig extends ReferencePluginConfig {

    @Name("zookeeperQuorum")
    @Nullable
    @Description("Zookeeper Quorum. By default it is set to 'localhost'")
    public String zkQuorum;

    @Name("zookeeperClientPort")
    @Nullable
    @Macro
    @Description("Zookeeper Client Port. By default it is set to 2181")
    public String zkClientPort;

    @Name("zookeeperParent")
    @Nullable
    @Macro
    @Description("Parent Node of HBase in Zookeeper. Default to '/hbase'")
    public String zkNodeParent;

    public CDCHBaseConfig(String referenceName) {
      super(referenceName);
    }
  }
}
