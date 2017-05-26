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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCHBase Sink")
@Description("Writes to Apache HBase tables.")
public class HBaseSink extends SparkCompute<StructuredRecord, Mutation> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
  private final HBaseSinkConfig hBaseSinkConfig;
  private HBaseTableUpdater hBaseTableUpdater;

  private Configuration conf;
  private Admin hBaseAdmin;
  private Connection connection;

  public HBaseSink(HBaseSinkConfig config) {
    this.hBaseSinkConfig = config;
  }

  private class TableConfig {
    String name;

  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    if (hBaseSinkConfig.containsMacro("zookeeperQuorum") || hBaseSinkConfig.containsMacro("name")) {
      return;
    }
  }

  private void createHBaseTable(String tableName) {
    try {
      hBaseAdmin.createTable(new HTableDescriptor(TableName.valueOf(tableName)));
    } catch (TableExistsException ex) {
      LOG.debug("HBase Table {} already exists.", tableName);
    } catch (IOException ex) {
      LOG.error("Unable to create a HBase Table.", ex);
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);

    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    // initialize configuration
    conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);
    String zkQuorum = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkQuorum()) ?
      hBaseSinkConfig.getZkQuorum() : "localhost";
    String zkClientPort = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkClientPort()) ?
      hBaseSinkConfig.getZkClientPort() : "2181";
    String zkNodeParent = !Strings.isNullOrEmpty(hBaseSinkConfig.getZkNodeParent()) ?
      hBaseSinkConfig.getZkNodeParent() : "/hbase";
    conf.addResource("hbase.mapred.output.quorum");
    conf.set("hbase.mapred.output.quorum", String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));

    // don't need to provide a table name here because we will be dynamically adding to tables
    connection = ConnectionFactory.createConnection(conf);
    hBaseAdmin = connection.getAdmin();
    hBaseTableUpdater = new HBaseTableUpdater();
  }

  public void destroy() throws IOException {
    // super.destroy();
    connection.close();
  }

  // TODO figure out what JavaRDD does
  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext input,
                                             JavaRDD<StructuredRecord> javaRDD)
    throws Exception {
    Schema recordSchema = input.getSchema();
    String tableName = input.get("table");
    if(recordSchema.getRecordName().equals("DDLRecord")) {
      assert(recordSchema.getField("table") != null);
      createHBaseTable(tableName);
    } else {
      hBaseTableUpdater.updateHBaseTable(input, connection.getTable(TableName.valueOf(tableName)));
    }

    // TODO figure out what to return
    return new JavaRDD<StructuredRecord>();
  }
}