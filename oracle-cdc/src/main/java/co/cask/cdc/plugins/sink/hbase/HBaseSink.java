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

package co.cask.cdc.plugins.sink.hbase;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCHBase Sink")
@Description("Writes to Apache HBase tables.")
public class HBaseSink extends ReferenceBatchSink<StructuredRecord, ImmutableBytesWritable, Mutation> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
  private final HBaseSinkConfig hBaseSinkConfig;
  private RecordMutationTransformer recordMutationTransformer;

  private Configuration conf;
  private HBaseAdmin hBaseAdmin;

  public HBaseSink(HBaseSinkConfig config) {
    super(config);
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

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);

    // don't need to provide a table name here because we will be dynamically adding to tables
    context.addOutput(null, new HBaseOutputFormatProvider(hBaseSinkConfig, conf));
  }

  private void createHBaseTable(String tableName) {
    try {
      hBaseAdmin = new HBaseAdmin(conf);
      hBaseAdmin.createTable(new HTableDescriptor(TableName.valueOf(tableName)));
    } catch (TableExistsException ex) {
      LOG.debug("HBase Table {} already exists.", tableName);
    } catch (IOException ex) {
      LOG.error("Unable to create a HBase Table.", ex);
    }
  }

  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      String zkQuorum = !Strings.isNullOrEmpty(config.getZkQuorum()) ? config.getZkQuorum() : "localhost";
      String zkClientPort = !Strings.isNullOrEmpty(config.getZkClientPort()) ? config.getZkClientPort() : "2181";
      String zkNodeParent = !Strings.isNullOrEmpty(config.getZkNodeParent()) ? config.getZkNodeParent() : "/hbase";
      conf.put(MultiTableOutputFormatWithQuorumAddress.QUORUM_ADDRESS,
               String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));
      String[] serializationClasses = {
        configuration.get("io.serializations"),
        MutationSerialization.class.getName(),
        ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName() };
      conf.put("io.serializations", StringUtils.arrayToString(serializationClasses));
    }

    @Override
    public String getOutputFormatClassName() {
      return MultiTableOutputFormatWithQuorumAddress.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordMutationTransformer = new RecordMutationTransformer();
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<ImmutableBytesWritable, Mutation>> emitter) throws Exception {
    Schema recordSchema = input.getSchema();
    if(recordSchema.getRecordName().equals("DDLRecord")) {
      assert(recordSchema.getField("table") != null);
      createHBaseTable(input.get("table").toString());
      // Do we have to emit anything after this?

      return;
    }
    Mutation mutation = recordMutationTransformer.toMutation(input);
    emitter.emit(new KeyValue<ImmutableBytesWritable, Mutation>(new ImmutableBytesWritable(Bytes.toBytes((String) input.get("table"))), mutation));
  }
}
