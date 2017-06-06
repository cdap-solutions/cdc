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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

import java.sql.Driver;
import java.util.HashMap;
import java.util.Map;

/**
 * Database sink for cdc
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCDatabaseSink")
@Description("Writes to database table using jdbc.")
public class DatabaseSink extends ReferenceBatchSink<StructuredRecord, DatabaseRecord, NullWritable> {
  private final DatabaseSinkConfig config;

  public DatabaseSink(DatabaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
  }

  public void validateJDBCPluginPipeline(PipelineConfigurer pipelineConfigurer, String jdbcPluginId) {
    Preconditions.checkArgument(!(config.user == null && config.password != null),
                                "user is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove password and retry.");
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass(config.jdbcPluginType,
                                                                                config.jdbcPluginName,
                                                                                jdbcPluginId,
                                                                                PluginProperties.builder().build());
    Preconditions.checkArgument(
      jdbcDriverClass != null, "Unable to load JDBC Driver class for plugin name '%s'. Please make sure that the " +
        "plugin '%s' of type '%s' containing the driver has been installed correctly.", config.jdbcPluginName,
      config.jdbcPluginName, config.jdbcPluginType);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", config.jdbcPluginType, config.jdbcPluginName);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DatabaseRecord, NullWritable>> emitter)
    throws Exception {
    // emit the structured record as is, DatabaseOutputFormat will make sure correct prepared statements are created
    // and DatabaseRecord will make sure correct values are filled in.
    emitter.emit(new KeyValue<>(new DatabaseRecord(input), NullWritable.get()));
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    context.addOutput(Output.of(config.referenceName, new DBOutputFormatProvider(config, driverClass)));
  }


  @Override
  public void destroy() {
  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DBOutputFormatProvider(DatabaseSinkConfig dbSinkConfig, Class<? extends Driver> driverClass) {
      this.conf = new HashMap<>();

      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
      conf.put(DBConfiguration.URL_PROPERTY, dbSinkConfig.connectionString);
      if (dbSinkConfig.user != null) {
        conf.put(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.user);
      }
      if (dbSinkConfig.password != null) {
        conf.put(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.password);
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return DatabaseOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
