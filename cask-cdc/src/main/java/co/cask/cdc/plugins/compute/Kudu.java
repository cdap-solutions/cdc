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

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Update;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spark compute plugin
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Kudu")
public class Kudu extends SparkCompute<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Kudu.class);
  private static Gson GSON = new Gson();
  private final KuduConfig kuduConfig;
  private final Map<String, Schema> schemaMapping = new HashMap<>();
  private final Set<String> existingTables = new HashSet<>();

  public Kudu(KuduConfig config) {
    this.kuduConfig = config;
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    return javaRDD.mapPartitions(new FlatMapFunction<Iterator<StructuredRecord>, StructuredRecord>() {

      @Override
      public Iterable<StructuredRecord> call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {
        try (KuduClient client = new KuduClient.KuduClientBuilder(kuduConfig.getMasterAddress())
          .defaultOperationTimeoutMs(kuduConfig.getOperationTimeout())
          .defaultAdminOperationTimeoutMs(kuduConfig.getAdministrationTimeout())
          .disableStatistics()
          .bossCount(kuduConfig.getThreads())
          .build()) {

          KuduSession session = client.newSession();
          // Buffer 100 operations
          session.setMutationBufferSpace(100);
          session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            LOG.info("Received StructuredRecord in Kudu {}", GSON.toJson(input));
            LOG.info("StructuredRecord to StringConverter Kudu {}", StructuredRecordStringConverter.toJsonString(input));
            if (input.getSchema().getRecordName().equals("DDLRecord")) {
              if (updateKuduTableSchema(client, input)) {
                // Schema for the table is updated. Flush the session now
                session.flush();
              }
            } else {
              updateKuduTableRecord(client, session, input);
            }
          }
        }

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

  private boolean updateKuduTableSchema(KuduClient client, StructuredRecord input) throws Exception {
    LOG.info("Updating Kudu Table Schema for {}", GSON.toJson(input));
    String namespacedTableName = input.get("table");
    Schema newSchema = Schema.parseJson((String) input.get("schema"));
    if (!existingTables.contains(namespacedTableName) && !client.tableExists(namespacedTableName)) {
      // Table does not exists in the Kudu yet.
      // Creation of table will be attempted when we first see the DML Record.
      // Since at that point we know the primary keys to used.
      schemaMapping.put(namespacedTableName, newSchema);
      return false;
    }

    KuduTable table = client.openTable(namespacedTableName);
    org.apache.kudu.Schema kuduTableSchema = table.getSchema();
    Set<String> oldColumns = new HashSet<>();
    for (ColumnSchema schema : kuduTableSchema.getColumns()) {
      oldColumns.add(schema.getName());
    }
    LOG.info("OldColumns {}", oldColumns);

    Set<String> newColumns = new HashSet<>();
    for (Schema.Field field : newSchema.getFields()) {
      newColumns.add(field.getName());
    }

    LOG.info("NewColumns {}", newColumns);

    Sets.SetView<String> columnDiff = Sets.symmetricDifference(newColumns, oldColumns);
    LOG.info("Column Diff {}", columnDiff);
    Set<String> columnsToDelete = new HashSet<>();
    Set<String> columnsToAdd = new HashSet<>();
    for (String column : columnDiff) {
      if (oldColumns.contains(column)) {
        // This column is removed
        columnsToDelete.add(column);
      } else {
        // This column is added
        columnsToAdd.add(column);
      }
    }

    LOG.info("Columns to add {} and Columns to delete {}", columnsToAdd, columnsToDelete);

    AlterTableOptions alterTableOptions = new AlterTableOptions();
    for (String column : columnsToDelete) {
      alterTableOptions.dropColumn(column);
      LOG.info("Deleting column {}", column);
    }

    for (String column : columnsToAdd) {
      Schema.Field newField = newSchema.getField(column);
      Type kuduType = toKuduType(column, newField.getSchema());
      alterTableOptions.addNullableColumn(column, kuduType);
      LOG.info("Adding column {} of type {} to the KuduTable.", column, kuduType);
    }

    if (!(columnsToAdd.isEmpty() && columnsToDelete.isEmpty())) {
      LOG.info("Altering table {}, {}",table.getName(), alterTableOptions);
      client.alterTable(table.getName(), alterTableOptions);
      client.isAlterTableDone(table.getName());
      LOG.info("Alter table done!");
      table = client.openTable(table.getName());
      LOG.info("Columns after alter table {}", table.getSchema().getColumns());
    }

    LOG.info("Returning!");
    return true;
  }

  private void updateKuduTableRecord(KuduClient client, KuduSession session, StructuredRecord input) throws Exception {
    String namespacedTableName = input.get("table");
    String operationType = input.get("op_type");
    List<String> primaryKeys = input.get("primary_keys");
    if (!existingTables.contains(namespacedTableName) && !client.tableExists(namespacedTableName)) {
      // TODO it is assumed that create table and operation on the table happens in the same batch
      // Most likely it will be case but they can come in separate batch
      createKuduTable(client, namespacedTableName, schemaMapping.get(namespacedTableName), primaryKeys);
      existingTables.add(namespacedTableName);
    }

    KuduTable table = client.openTable(namespacedTableName);
    StructuredRecord change = input.get("change");
    List<Schema.Field> fields = Schema.parseJson((String)input.get("schema")).getFields();
    switch (operationType) {
      case "I":
        Insert insert = table.newInsert();
        for (Schema.Field field : fields) {
          // Normalizer always passes the full schema, however it is possible that only few fields are provided
          // Check if the field is actually provided
          if (change.get(field.getName()) != null) {
            addColumnDataBasedOnType(insert.getRow(), field, change.get(field.getName()));
          }
        }
        session.apply(insert);
        break;
      case "U":
        Update update = table.newUpdate();
        for (Schema.Field field : fields) {
          // Normalizer always passes the full schema, however it is possible that only few fields are provided
          // Check if the field is actually provided
          if (change.get(field.getName()) != null) {
            addColumnDataBasedOnType(update.getRow(), field, change.get(field.getName()));
          }
        }
        session.apply(update);
        break;
      case "D":
        // TODO DELETE operation requires primary keys always
        Delete delete = table.newDelete();
        for (String keyColumn : primaryKeys) {
          for (Schema.Field field : fields) {
            // Normalizer always passes the full schema, however it is possible that only few fields are provided
            // Check if the field is actually provided
            if (field.getName().equals(keyColumn) && change.get(field.getName()) != null) {
              addColumnDataBasedOnType(delete.getRow(), field, change.get(field.getName()));
              break;
            }
          }
        }
        session.apply(delete);
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);
    }
  }

  private void addColumnDataBasedOnType(PartialRow row, co.cask.cdap.api.data.schema.Schema.Field field,
                                        Object value) throws TypeConversionException {
    String columnName = field.getName();
    Type type = toKuduType(field.getName(), field.getSchema());
    switch (type) {
      case STRING:
        row.addString(columnName, (String) value);
        break;
      case INT32:
        row.addInt(columnName, (int) value);
        break;
      case INT64:
        row.addLong(columnName, (long) value);
        break;
      case BINARY:
        if (value instanceof ByteBuffer) {
          row.addBinary(columnName, (ByteBuffer) value);
        } else {
          row.addBinary(columnName, (byte[]) value);
        }
        break;
      case DOUBLE:
        row.addDouble(columnName, (double) value);
        break;
      case FLOAT:
        row.addFloat(columnName, (float) value);
        break;
      case BOOL:
        row.addBoolean(columnName, (boolean) value);
        break;
      default:
        throw new RuntimeException(String.format("Unexpected Kudu type '%s' found.", type));
    }
  }

  private void createKuduTable(KuduClient client, String tableName, co.cask.cdap.api.data.schema.Schema writeSchema,
                               List<String> primaryKeys) throws Exception {
    // Check if the table exists, if table does not exist, then create one
    // with schema defined in the write schema.
    try {
      if (!client.tableExists(tableName)) {
        // Convert the writeSchema into Kudu schema.
        List<ColumnSchema> columnSchemas = toKuduSchema(writeSchema.getFields(), new HashSet<>(primaryKeys),
                                                        kuduConfig.getCompression(), kuduConfig.getEncoding());
        org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(getOrderedSchemaColumns(primaryKeys, columnSchemas));
        CreateTableOptions options = new CreateTableOptions();
        options.addHashPartitions(primaryKeys, kuduConfig.getBuckets(), kuduConfig.getSeed());

        try {
          KuduTable table = client.createTable(tableName, kuduSchema, options);
          LOG.info("Successfully created Kudu table '{}', Table ID '{}'", tableName, table.getTableId());
        } catch (KuduException e) {
          throw new RuntimeException(
            String.format("Unable to create table '%s'. Reason : %s", tableName, e.getMessage())
          );
        }
      }
    } catch (KuduException e) {
      String msg = String.format("Unable to check if the table '%s' exists in kudu. Reason : %s", tableName,
                                 e.getMessage());
      LOG.warn(msg);
      throw new RuntimeException(e);
    } catch (TypeConversionException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  // Create list of ColumnSchema where columns corresponding to the primary key will appear first.
  // This is required because of column ordering constraing in Kudu (https://issues.apache.org/jira/browse/KUDU-1271)
  private List<ColumnSchema> getOrderedSchemaColumns(List<String> primaryKeys, List<ColumnSchema> columnSchemas) {
    if (primaryKeys.isEmpty()) {
      return columnSchemas;
    }

    List<ColumnSchema> orderedColumnSchemas = new ArrayList<>();
    // First insert all the columns corresponding to the primary key
    for (String key : primaryKeys) {
      for (ColumnSchema columnSchema : columnSchemas) {
        if (columnSchema.getName().equals(key)) {
          orderedColumnSchemas.add(columnSchema);
          break;
        }
      }
    }

    HashSet<String> keySet = new HashSet<>(primaryKeys);
    for (ColumnSchema columnSchema : columnSchemas) {
      if (!keySet.contains(columnSchema.getName())) {
        orderedColumnSchemas.add(columnSchema);
      }
    }
    return orderedColumnSchemas;
  }

  /**
   * Converts from CDAP field types to Kudu types.
   *
   * @param fields CDAP Schema fields
   * @param columns List of columns that are considered as keys
   * @param algorithm Compression algorithm to be used for the column.
   * @param encoding Encoding type
   * @return List of {@link ColumnSchema}
   * @throws TypeConversionException thrown when CDAP schema cannot be converted to Kudu Schema.
   */
  private List<ColumnSchema> toKuduSchema(List<co.cask.cdap.api.data.schema.Schema.Field> fields, Set<String> columns,
                                          ColumnSchema.CompressionAlgorithm algorithm,
                                          ColumnSchema.Encoding encoding)
    throws TypeConversionException {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    for (co.cask.cdap.api.data.schema.Schema.Field field : fields) {
      String name = field.getName();
      Type kuduType = toKuduType(name, field.getSchema());
      ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, kuduType);
      if (field.getSchema().isNullable() && !columns.contains(name)) {
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

  /**
   * Convert from {@link co.cask.cdap.api.data.schema.Schema.Type} to {@link Type}.
   *
   * @param schema {@link StructuredRecord} field schema.
   * @return {@link Type} Kudu type.
   * @throws TypeConversionException thrown when can't be converted.
   */
  private Type toKuduType(String name, co.cask.cdap.api.data.schema.Schema schema) throws TypeConversionException {
    co.cask.cdap.api.data.schema.Schema.Type type = schema.getType();
    if (type == co.cask.cdap.api.data.schema.Schema.Type.STRING) {
      return Type.STRING;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.INT) {
      return Type.INT32;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.LONG) {
      return Type.INT64;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.BYTES) {
      return Type.BINARY;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.DOUBLE) {
      return Type.DOUBLE;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.FLOAT) {
      return Type.FLOAT;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.BOOLEAN) {
      return Type.BOOL;
    } else if (type == co.cask.cdap.api.data.schema.Schema.Type.UNION) { // Recursively drill down into the non-nullable type.
      return toKuduType(name, schema.getNonNullable());
    } else {
      throw new TypeConversionException(
        String.format("Field '%s' is having a type '%s' that is not supported by Kudu. Please change the type.",
                      name, type.toString())
      );
    }
  }
}
