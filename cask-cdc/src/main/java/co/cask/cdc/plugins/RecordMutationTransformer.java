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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;

import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class RecordMutationTransformer {
  private final String columnFamily = "cdc";

  public Mutation toMutation(StructuredRecord record) {
    // a DML record
    List<String> primaryKeys = record.get("primary_keys");
    String opType = record.get("op_type");
    StructuredRecord change = record.get("change");
    String rowKey = "";
    for(String primaryKey : primaryKeys) {
      rowKey.concat(change.get(primaryKey).toString());
    }

    // choose operation type
    switch (opType) {
      case "I":
      case "U":
        Put put;
        put = new Put(Bytes.toBytes(rowKey));
        for (Schema.Field field : change.getSchema().getFields()) {
          setPutField(put, columnFamily, field, change);
        }
        return put;
      case "D":
        Delete delete;
        delete = new Delete(Bytes.toBytes(rowKey));
        for (Schema.Field field : change.getSchema().getFields()) {
          delete.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field.getName()));
        }
        return delete;
      default:
        throw new IllegalArgumentException(opType + "can only be \"I\" (insert), \"U\" (update), or \"D\" (delete)");
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
}