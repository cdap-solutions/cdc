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
import co.cask.cdc.common.KuduSinkConfig;
import co.cask.cdc.common.TypeConversionException;
import co.cask.hydrator.common.ReferenceBatchSink;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.hadoop.io.NullWritable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Update;
import org.apache.kudu.mapreduce.KuduTableOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("CDCKuduSink")
@Description("Writes to Apache Kudu tables.")
public class Kudu extends ReferenceBatchSink<StructuredRecord, NullWritable, Operation> {
  private static final Logger LOG = LoggerFactory.getLogger(Kudu.class);
  private static Gson GSON = new Gson();
  private final KuduSinkConfig kuduSinkConfig;

  // Kudu client and table.
  private KuduClient client;
  private KuduTable table;

  // Write schema associated with the pipeline.
  private Schema outputSchema;

  public Kudu(KuduSinkConfig config) {
    super(config);
    this.kuduSinkConfig = config;
  }

  /**
   * Configures the plugin.
   *
   * <p>
   *   Checks if the Kudu table exists, if the table doesn't exist then a valid Kudu table gets created.
   *   If the table exists, then the schema is compared.
   * </p>
   * @param configurer Handler to schema and other aspects of pipeline.
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(kuduSinkConfig.optSchema), "Write schema is not specified. Please add" +
      "the write schema.");

    // Checks if that we are writing with has been constructed correctly.
    Schema writeSchema = kuduSinkConfig.getSchema();
    configurer.getStageConfigurer().setOutputSchema(writeSchema);

    // If there is macro specified for 'master' address or table name, then
    // we defer the creation of table to initialize.
    if (kuduSinkConfig.containsMacro("master") || kuduSinkConfig.containsMacro("name")) {
      return;
    }
    createKuduTable();
  }

  /**
   * Initializes the plugin by initiating connection to Kudu using the client.
   *
   * @param context of the plugin.
   */
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // Parsing the schema should never fail here, because configure has validated it.
    outputSchema = kuduSinkConfig.getSchema();
    client = new KuduClient.KuduClientBuilder(kuduSinkConfig.getMasterAddress())
      .defaultOperationTimeoutMs(kuduSinkConfig.getOperationTimeout())
      .defaultAdminOperationTimeoutMs(kuduSinkConfig.getAdministrationTimeout())
      .disableStatistics()
      .bossCount(kuduSinkConfig.getThreads())
      .build();

    table = client.openTable(kuduSinkConfig.getTableName());
  }

  /**
   * Creates a Kudu table if it doesn't exist.
   */
  private void createKuduTable() {
    // Create a Kudu connection. A connection is attempted during the
    // deployment of the pipeline that contains this plugin.
    // NOTE: I am not sure if this is the right place for this to happen, but
    // not sure if it's the right place during initialization to create the
    // table if it doesn't exit.
    KuduClient localClient = new KuduClient.KuduClientBuilder(kuduSinkConfig.getMasterAddress())
      .defaultOperationTimeoutMs(kuduSinkConfig.getOperationTimeout())
      .defaultAdminOperationTimeoutMs(kuduSinkConfig.getAdministrationTimeout())
      .disableStatistics()
      .bossCount(kuduSinkConfig.getThreads())
      .build();

    Schema writeSchema = kuduSinkConfig.getSchema();
    // Check if the table exists, if table does not exist, then create one
    // with schema defined in the write schema.
    try {
      if (!localClient.tableExists(this.kuduSinkConfig.getTableName())) {
        // Convert the writeSchema into Kudu schema.
        List<ColumnSchema> columnSchemas = toKuduSchema(writeSchema, kuduSinkConfig.getColumns(),
                                                        kuduSinkConfig.getCompression(), kuduSinkConfig.getEncoding());
        org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(columnSchemas);
        CreateTableOptions options = new CreateTableOptions();
        options.addHashPartitions(new ArrayList<>(kuduSinkConfig.getColumns()), kuduSinkConfig.getBuckets(), kuduSinkConfig.getSeed());

        try {
          KuduTable table =
            localClient.createTable(kuduSinkConfig.getTableName(), kuduSchema, options);
          LOG.info("Successfully create Kudu table '%s', Table ID '%s'", kuduSinkConfig.getTableName(), table.getTableId());
        } catch (KuduException e) {
          throw new RuntimeException(
            String.format("Unable to create table '%s'. Reason : %s", kuduSinkConfig.getTableName(), e.getMessage())
          );
        }
      } else {
        // If the table exists in Kudu, compare the schema and make sure they are the same.
        // If they are not the same then throw an exception.
        KuduTable table = localClient.openTable(kuduSinkConfig.getTableName());
        org.apache.kudu.Schema kuduSchema = table.getSchema();
        checkSchemaCompatibility(kuduSchema, writeSchema);
      }
    } catch (KuduException e) {
      String msg = String.format("Unable to check if the table '%s' exists in kudu. Reason : %s",
                                 kuduSinkConfig.getTableName(), e.getMessage());
      LOG.warn(msg);
      throw new RuntimeException(e);
    } catch (TypeConversionException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      if (localClient != null) {
        try {
          localClient.close();
        } catch (KuduException e) {
          LOG.warn("Failed to close kudu client connection. {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Checks if Kudu Schema for an existing table is same as the write schema specified by the pipeline.
   *
   * @param kuduSchema field of Kudu schema.
   * @param writeSchema fields of pipeline schema.
   */
  private void checkSchemaCompatibility(org.apache.kudu.Schema kuduSchema, Schema writeSchema) {
    if (kuduSchema.getColumns().size() != writeSchema.getFields().size()) {
      throw new RuntimeException(
        String.format("Kudu table schema and write schema do not have same number of columns. " +
                        "Please fix schema and re-submit.")
      );
    }

    // Iterate through all the field in the Kudu table.
    for (org.apache.kudu.ColumnSchema kschema : kuduSchema.getColumns()) {
      String kName = kschema.getName();
      org.apache.kudu.Type kType = kschema.getType();

      // Check if field name matches.
      if (writeSchema.getField(kName) == null) {
        throw new RuntimeException(
          String.format("Kudu table '%s' has a field '%s' that does not exist in your write schema. Please" +
                          "make the appropriate change and re-submit the pipeline.", kuduSinkConfig.optTableName,
                        kName)
        );
      } else {
        // If it matches, check if the type matches.
        try {
          if (!toKuduType(kName, writeSchema.getField(kName).getSchema()).equals(kType)) {
            throw new RuntimeException(
              String.format("Kudu table '%s' has a field '%s' that does not match the type in your write schema. " +
                              "Please change the type of field '%s' and re-submit",
                            kuduSinkConfig.getTableName(), kName, kName)
            );
          }
        } catch (TypeConversionException e) {
          throw new RuntimeException(
            String.format("Kudu table '%s' has a field '%s' that has type that is not supported by kudu. Please" +
                            "change the type of field '%s' to one supported by Kudu and re-submit",
                          kuduSinkConfig.getTableName(), kName, kName)
          );
        }
      }
    }
  }

  /**
   * Convert from {@link co.cask.cdap.api.data.schema.Schema.Type} to {@link Type}.
   *
   * @param schema {@link StructuredRecord} field schema.
   * @return {@link Type} Kudu type.
   * @throws TypeConversionException thrown when can't be converted.
   */
  private Type toKuduType(String name, Schema schema) throws TypeConversionException {
    Schema.Type type = schema.getType();
    if (type == Schema.Type.STRING) {
      return Type.STRING;
    } else if (type == Schema.Type.INT) {
      return Type.INT32;
    } else if (type == Schema.Type.LONG) {
      return Type.INT64;
    } else if (type == Schema.Type.BYTES) {
      return Type.BINARY;
    } else if (type == Schema.Type.DOUBLE) {
      return Type.DOUBLE;
    } else if (type == Schema.Type.FLOAT) {
      return Type.FLOAT;
    } else if (type == Schema.Type.BOOLEAN) {
      return Type.BOOL;
    } else if (type == Schema.Type.UNION) { // Recursively drill down into the non-nullable type.
      return toKuduType(name, schema.getNonNullable());
    } else {
      throw new TypeConversionException(
        String.format("Field '%s' is having a type '%s' that is not supported by Kudu. Please change the type.",
                      name, type.toString())
      );
    }
  }

  /**
   * Converts from CDAP field types to Kudu types.
   *
   * @param schema CDAP Schema
   * @param columns List of columns that are considered as keys
   * @param algorithm Compression algorithm to be used for the column.
   * @param encoding Encoding type
   * @return List of {@link ColumnSchema}
   * @throws TypeConversionException thrown when CDAP schema cannot be converted to Kudu Schema.
   */
  private List<ColumnSchema> toKuduSchema(Schema schema, Set<String> columns,
                                          ColumnSchema.CompressionAlgorithm algorithm,
                                          ColumnSchema.Encoding encoding)
    throws TypeConversionException {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      Type kuduType = toKuduType(name, field.getSchema());
      ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, kuduType);
      if (field.getSchema().isNullable()) {
        builder.nullable(true);
      }
      builder.encoding(encoding);
      builder.compressionAlgorithm(algorithm);
      if (columns.contains(name)) {
        builder.key(true);
      }
      columnSchemas.add(builder.build());
    }
    return columnSchemas;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // If there was a macro specified, then we attempt to create the
    // table here during initialization. If it's not a macro, then we
    // just open the the table and proceed.
    createKuduTable();
    context.addOutput(Output.of(kuduSinkConfig.referenceName, new KuduOutputFormatProvider(kuduSinkConfig)));
  }

  /**
   * Transform the {@link StructuredRecord} into the Kudu operations.
   *
   * @param input A single {@link StructuredRecord} instance
   * @param emitter for emitting records to Kudu Output format.
   */
  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Operation>> emitter) throws Exception {
    LOG.info("Input StructuredRecord is {}", GSON.toJson(input));
    if (input.getSchema().getRecordName().equals("DDLRecord")) {
      String schemaString = input.get("schema");
      Schema newSchema = Schema.parseJson(schemaString);

      // Identified that it is a DDL Record - supports adding new columns, deleting columns
      // TODO : Add support for renaming column (is it possible that two columns can be renamed in same DDLRecord?)
      org.apache.kudu.Schema kuduTableSchema = table.getSchema();
      Set<String> oldColumns = new HashSet<>();
      for (ColumnSchema schema : kuduTableSchema.getColumns()) {
        oldColumns.add(schema.getName());
      }

      Set<String> newColumns = new HashSet<>();
      for (Schema.Field field : newSchema.getFields()) {
        newColumns.add(field.getName());
      }

      if (newColumns.size() > oldColumns.size()) {
        // columns have been added
        newColumns.removeAll(oldColumns);
        for (String columnName : newColumns) {
          Schema.Field newField = newSchema.getField(columnName);
          Type kuduType = toKuduType(columnName, newField.getSchema());
          // add nullable column since we don't have default value?
          client.alterTable(table.getName(), new AlterTableOptions().addNullableColumn(columnName, kuduType));
          client.isAlterTableDone(table.getName());
        }
      } else {
        oldColumns.removeAll(newColumns);
        // columns have been removed
        for (String columnName : oldColumns) {
          client.alterTable(table.getName(), new AlterTableOptions().dropColumn(columnName));
          client.isAlterTableDone(table.getName());
        }
      }
      return;
    }

    String operationType = input.get("op_type");
    switch (operationType) {
      case "I":
        Insert insert = table.newInsert();
        Map<String, String> insertData = input.get("after");
        for (Map.Entry<String, String> entry : insertData.entrySet()) {
          insert.getRow().addString(entry.getKey(), entry.getValue());
        }
        emitter.emit(new KeyValue<NullWritable, Operation>(NullWritable.get(), insert));
        break;
      case "U":
        Update update = table.newUpdate();
        Map<String, String> updateData = input.get("after");
        for (Map.Entry<String, String> entry : updateData.entrySet()) {
          update.getRow().addString(entry.getKey(), entry.getValue());
        }
        emitter.emit(new KeyValue<NullWritable, Operation>(NullWritable.get(), update));
        break;
      case "D":
        Delete delete = table.newDelete();
        Map<String, String> deleteData = input.get("before");
        Set<String> keyColumns = kuduSinkConfig.getColumns();
        for (String keyColumn : keyColumns) {
          delete.getRow().addString(keyColumn, deleteData.get(keyColumn));
        }
        // for (Map.Entry<String, String> entry : deleteData.entrySet()) {
        //  delete.getRow().addString(entry.getKey(), entry.getValue());
        // }
        emitter.emit(new KeyValue<NullWritable, Operation>(NullWritable.get(), delete));
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);
    }
  }

  /**
   * Adds a field to the row from the {@link StructuredRecord}.
   *
   * @param row Kudu Row.
   * @param name of the field.
   * @param schema of the field.
   * @param val to be added to the field.
   */
  private void createKuduRow(PartialRow row, String name, Schema schema, Object val) {
    switch (schema.getType()) {
      case BOOLEAN:
        row.addBoolean(name, (Boolean) val);
        break;
      case INT:
        row.addInt(name, (Integer) val);
        break;
      case LONG:
        row.addLong(name, (Long) val);
        break;
      case FLOAT:
        row.addFloat(name, (Float) val);
        break;
      case DOUBLE:
        row.addDouble(name, (Double) val);
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          row.addBinary(name, (ByteBuffer) val);
        } else {
          row.addBinary(name, (byte[]) val);
        }
        break;
      case STRING:
        row.addString(name, (String) val);
        break;
      case UNION: // Recursively drill down to find the type.
        createKuduRow(row, name, schema.getNonNullable(), val);
        break;
      default:
        throw new IllegalArgumentException(
          "Field '" + name + "' is of unsupported type '" + schema.getType() + "' by Kudu."
        );
    }
  }

  /**
   * Called when the run is completed, release all the resources acquired during initialize.
   *
   * @param succeeded provides the status of the run.
   * @param context context to this plugin.
   */
  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    try {
      if (client != null) {
        client.close();
      }
    } catch (KuduException e) {
      LOG.warn("There was a problem closing kudu client. Reason : {}", e.getMessage());
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }


  /**
   * Provider for Kudu Output format.
   */
  private class KuduOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    KuduOutputFormatProvider(KuduSinkConfig kuduSinkConfig) throws IOException {
      this.conf = new HashMap<>();
      this.conf.put("kudu.mapreduce.master.addresses", kuduSinkConfig.getMasterAddress());
      this.conf.put("kudu.mapreduce.output.table", kuduSinkConfig.getTableName());
      this.conf.put("kudu.mapreduce.operation.timeout.ms", String.valueOf(kuduSinkConfig.getOperationTimeout()));
      this.conf.put("kudu.mapreduce.buffer.row.count", kuduSinkConfig.optFlushRows);
    }

    @Override
    public String getOutputFormatClassName() {
      return KuduTableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}

