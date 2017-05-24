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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class RecordMutationTransformer {
  private final String rowField;
  private final String opTypeField;
  private final String beforeField;
  private final String afterField;
  private final Schema outputSchema;

  public RecordMutationTransformer(String rowField, String opTypeField, String beforeField, String afterField,
                                   @Nullable Schema outputSchema) {
    if (outputSchema != null) {
      validateSchema(rowField, outputSchema);
    }
    this.rowField = rowField;
    this.opTypeField = opTypeField;
    this.beforeField = beforeField;
    this.afterField = afterField;
    this.outputSchema = outputSchema;
  }

  private void validateSchema(String rowField, Schema outputSchema) {
    if (outputSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException(
        String.format("Schema must be a record instead of '%s'.", outputSchema.getType()));
    }
    Schema.Field schemaRowField = outputSchema.getField(rowField);
    if (schemaRowField == null) {
      throw new IllegalArgumentException("Row field must be present in the schema");
    }
    if (!schemaRowField.getSchema().isSimpleOrNullableSimple()) {
      throw new IllegalArgumentException("Row field must be a simple type");
    }

    for (Schema.Field field : outputSchema.getFields()) {
      if (!field.getSchema().isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException(
          "Schema must only contain simple fields (boolean, int, long, float, double, bytes, string)");
      }
    }
  }

  public Mutation toMutation(StructuredRecord record, String family) {
    Schema recordSchema = record.getSchema();
    Preconditions.checkArgument(recordSchema.getType() == Schema.Type.RECORD, "input must be a record.");

    Schema.Field rowKeyField = getRowKeyField(recordSchema);
    Schema.Field opTypeKeyField = getOpTypeKeyField(recordSchema);
    Schema.Field beforeKeyField = getBeforeKeyField(recordSchema);
    Schema.Field afterKeyField = getAfterKeyField(recordSchema);
    Preconditions.checkArgument(rowKeyField != null, "Could not find row key field in record.");

    // choose operation type, only supports insert and delete now
    Put put;
    Delete delete;
    switch ((String) record.get(opTypeKeyField.getName())) {
      case "I":
      case "U":
        put = createPut(record, rowKeyField);
        StructuredRecord insertMap = record.get(afterKeyField.getName());
        for (Schema.Field field : insertMap.getSchema().getFields()) {
          setPutField(put, family, field, insertMap);
        }
        return put;
      case "D":
        delete = createDelete(record, rowKeyField);
        StructuredRecord deleteMap = record.get(beforeKeyField.getName());
        for (Schema.Field field : deleteMap.getSchema().getFields()) {
          delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()));
        }
        return delete;
      default:
        throw new IllegalArgumentException(opTypeKeyField.getName() +
                                             "can only be \"I\" (insert), \"U\" (update), or \"D\" (delete)");
    }
  }

  private void setPutField(Put put, String family, Schema.Field field, StructuredRecord record) {
    // have to handle nulls differently. In a Put object, it's only valid to use the add(byte[], byte[])
    // for null values, as the other add methods take boolean vs Boolean, int vs Integer, etc.
    String query = field.getName();
    Object val = record.get(field.getName());
    if (field.getSchema().isNullable() && val == null) {
      put.add(Bytes.toBytes(family), Bytes.toBytes(query), null);
      return;
    }

    Schema.Type type = validateAndGetType(field);

    switch (type) {
      case BOOLEAN:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Boolean) val));
        break;
      case INT:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Integer) val));
        break;
      case LONG:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Long) val));
        break;
      case FLOAT:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Float) val));
        break;
      case DOUBLE:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((Double) val));
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((ByteBuffer) val));
        } else {
          put.add(Bytes.toBytes(family), Bytes.toBytes(query), (byte[]) val);
        }
        break;
      case STRING:
        put.add(Bytes.toBytes(family), Bytes.toBytes(query), Bytes.toBytes((String) val));
        break;
      default:
        throw new IllegalArgumentException("Field " + field.getName() + " is of unsupported type " + type);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private Put createPut(StructuredRecord record, Schema.Field keyField) {
    String keyFieldName = keyField.getName();
    Object val = record.get(keyFieldName);
    Preconditions.checkArgument(val != null, "Row key cannot be null.");

    Schema.Type keyType = validateAndGetType(keyField);
    switch (keyType) {
      case BOOLEAN:
        return new Put(Bytes.toBytes((Boolean) val));
      case INT:
        return new Put(Bytes.toBytes((Integer) val));
      case LONG:
        return new Put(Bytes.toBytes((Long) val));
      case FLOAT:
        return new Put(Bytes.toBytes((Float) val));
      case DOUBLE:
        return new Put(Bytes.toBytes((Double) val));
      case BYTES:
        if (val instanceof ByteBuffer) {
          return new Put(Bytes.toBytes((ByteBuffer) val));
        } else {
          return new Put((byte[]) val);
        }
      case STRING:
        return new Put(Bytes.toBytes((String) record.get(keyFieldName)));
      default:
        throw new IllegalArgumentException("Row key is of unsupported type " + keyType);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private Delete createDelete(StructuredRecord record, Schema.Field keyField) {
    String keyFieldName = keyField.getName();
    Object val = record.get(keyFieldName);
    Preconditions.checkArgument(val != null, "Row key cannot be null.");

    Schema.Type keyType = validateAndGetType(keyField);
    switch (keyType) {
      case BOOLEAN:
        return new Delete(Bytes.toBytes((Boolean) val));
      case INT:
        return new Delete(Bytes.toBytes((Integer) val));
      case LONG:
        return new Delete(Bytes.toBytes((Long) val));
      case FLOAT:
        return new Delete(Bytes.toBytes((Float) val));
      case DOUBLE:
        return new Delete(Bytes.toBytes((Double) val));
      case BYTES:
        if (val instanceof ByteBuffer) {
          return new Delete(Bytes.toBytes((ByteBuffer) val));
        } else {
          return new Delete((byte[]) val);
        }
      case STRING:
        return new Delete(Bytes.toBytes((String) record.get(keyFieldName)));
      default:
        throw new IllegalArgumentException("Row key is of unsupported type " + keyType);
    }
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

  @Nullable
  private Schema.Field getRowKeyField(Schema recordSchema) {
    return recordSchema.getField(this.rowField);
  }

  @Nullable
  private Schema.Field getOpTypeKeyField(Schema recordSchema) {
    return recordSchema.getField(this.opTypeField);
  }

  @Nullable
  private Schema.Field getBeforeKeyField(Schema recordSchema) {
    return recordSchema.getField(this.beforeField);
  }

  @Nullable
  private Schema.Field getAfterKeyField(Schema recordSchema) {
    return recordSchema.getField(this.afterField);
  }
}
