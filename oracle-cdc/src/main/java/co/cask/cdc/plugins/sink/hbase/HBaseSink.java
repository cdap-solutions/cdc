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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
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
public class HBaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Mutation> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
  private static final Gson GSON = new Gson();
  private final HBaseSinkConfig hBaseSinkConfig;

  private Configuration conf;
  private RecordMutationTransformer recordMutationTransformer;
  private HBaseAdmin hBaseAdmin;

  public HBaseSink(HBaseSinkConfig config) {
    super(config);
    this.hBaseSinkConfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    if (hBaseSinkConfig.containsMacro("zookeeperQuorum") || hBaseSinkConfig.containsMacro("name")) {
      return;
    }
    createHBaseTable();
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

    createHBaseTable();
    context.addOutput(Output.of(hBaseSinkConfig.getTableName(), new HBaseOutputFormatProvider(hBaseSinkConfig, conf)));
  }

  private void createHBaseTable() {
    try {
      hBaseAdmin = new HBaseAdmin(conf);
      hBaseAdmin.createTable(new HTableDescriptor(TableName.valueOf(hBaseSinkConfig.getTableName())));
    } catch (TableExistsException ex) {
      LOG.debug("HBase Table {} already exists.", hBaseSinkConfig.getTableName());
    } catch (IOException ex) {
      LOG.error("Unable to create a HBase Table.", ex);
    }
  }

  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      conf.put(TableOutputFormat.OUTPUT_TABLE, config.getTableName());
      String zkQuorum = !Strings.isNullOrEmpty(config.getZkQuorum()) ? config.getZkQuorum() : "localhost";
      String zkClientPort = !Strings.isNullOrEmpty(config.getZkClientPort()) ? config.getZkClientPort() : "2181";
      String zkNodeParent = !Strings.isNullOrEmpty(config.getZkNodeParent()) ? config.getZkNodeParent() : "/hbase";
      conf.put(TableOutputFormat.QUORUM_ADDRESS, String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));
      String[] serializationClasses = {
        configuration.get("io.serializations"),
        MutationSerialization.class.getName(),
        ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName() };
      conf.put("io.serializations", StringUtils.arrayToString(serializationClasses));
    }

    @Override
    public String getOutputFormatClassName() {
      return TableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordMutationTransformer = new RecordMutationTransformer(hBaseSinkConfig.getRowField(), null);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    Mutation mutation = recordMutationTransformer.toMutation(input);
    if (mutation instanceof org.apache.hadoop.hbase.client.Put) {

    }
    org.apache.hadoop.hbase.client.Put hbasePut = new org.apache.hadoop.hbase.client.Put(mutation.getRow());
    for (Map.Entry<byte[], byte[]> entry : mutation.getValues().entrySet()) {
      hbasePut.add(hBaseSinkConfig.getColFamily().getBytes(), entry.getKey(), entry.getValue());
    }
    emitter.emit(new KeyValue<NullWritable, Mutation>(NullWritable.get(), hbasePut));
  }
}
