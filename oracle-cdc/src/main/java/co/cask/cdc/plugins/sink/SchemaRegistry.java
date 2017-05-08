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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;

/**
 * Sink used to write to the Schema registry
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("SchemaRegistry1")
@Description("Writes to the Schema registry")
public class SchemaRegistry extends BatchSink<StructuredRecord, byte[], Put>  {

  private static final byte[] COLUMN = Bytes.toBytes("schema");

  private final SchemaRegistryConfig config;


  public SchemaRegistry(SchemaRegistryConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    if (!config.containsMacro(config.schemaRegistry)) {
      pipelineConfigurer.createDataset(config.schemaRegistry, Table.class.getName(), DatasetProperties.EMPTY);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    if (!context.datasetExists(config.schemaRegistry)) {
      context.createDataset(config.schemaRegistry, Table.class.getName(), DatasetProperties.EMPTY);
    }
    context.addOutput(Output.ofDataset(config.schemaRegistry));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    if (input.getSchema().getRecordName().equals("DDLRecord")) {
      String namespacedTableName = input.get("table_name");
      long schemaHashId = input.get("schemaHashId");
      String schema = input.get("schema");
      String rowKey = namespacedTableName + ":" + schemaHashId;
      Put put = new Put(Bytes.toBytes(rowKey));
      put.add(COLUMN, Bytes.toBytes(schema));
      emitter.emit(new KeyValue<>(put.getRow(), put));
    }
  }

  /**
   * Configurations for the Schema registry
   */
  public static class SchemaRegistryConfig extends PluginConfig {
    @Name("name")
    @Description("Name of the dataset containing Schema registry.")
    @Macro
    String schemaRegistry;
  }
}
