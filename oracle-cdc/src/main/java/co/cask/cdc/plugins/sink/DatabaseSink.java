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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Database sink for cdc
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCDatabaseSink")
@Description("Writes to database table using jdbc.")
public class DatabaseSink extends ReferenceBatchSink<StructuredRecord, DatabaseRecord, NullWritable> {
  static final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  private final DatabaseSinkConfig config;

  public DatabaseSink(DatabaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.outputschema), "Output schema is not " +
      "specified. Please add the output schema.");

    // Checks if that we are writing with has been constructed correctly.
    Schema writeSchema = config.getSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(writeSchema);
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
    context.addOutput(Output.of(config.referenceName, new DBOutputFormatProvider(config)));
  }


  @Override
  public void destroy() {
  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DBOutputFormatProvider(DatabaseSinkConfig dbSinkConfig) {
      this.conf = new HashMap<>();

      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, JDBC_DRIVER);
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
