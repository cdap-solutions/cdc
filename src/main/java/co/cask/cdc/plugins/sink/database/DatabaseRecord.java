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

package co.cask.cdc.plugins.sink.database;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Database writable
 */
public class DatabaseRecord implements Writable, DBWritable, Configurable {

  private StructuredRecord record;
  private Configuration conf;

  /**
   * Used to construct a DBRecord from a StructuredRecord in the ETL Pipeline
   *
   * @param record the {@link StructuredRecord} to construct the {@link DatabaseRecord} from
   */
  public DatabaseRecord(StructuredRecord record) {
    this.record = record;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public DatabaseRecord() {
  }

  public void readFields(DataInput in) throws IOException {
    // no-op, since we may never need to support a scenario where you read a DBRecord from a non-RDBMS source
  }

  /**
   * @return the {@link StructuredRecord} contained in this object
   */
  public StructuredRecord getRecord() {
    return record;
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public void readFields(ResultSet resultSet) throws SQLException {
    // we do not need to read fields since we are only using this for sink
  }

  public void write(DataOutput out) throws IOException {
    // no-op
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    String operationType = record.get("op_type");
    List<String> primaryKeys = record.get("primary_keys");
    StructuredRecord change = record.get("change");
    List<Schema.Field> fieldNames = change.getSchema().getFields();
    switch (operationType) {
      case "I":
        int i = 1;
        fillInRecordValues(stmt, change, fieldNames, i);
        break;
      case "U":
        i = 1;
        // fill in updated values
        i = fillInRecordValues(stmt, change, fieldNames, i);

        // fill in primary key values
        fillInPrimaryKeyValues(stmt, primaryKeys, change, i);
        break;
      case "D":
        i = 1;
        fillInPrimaryKeyValues(stmt, primaryKeys, change, i);
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);

    }
  }

  private int fillInRecordValues(PreparedStatement stmt, StructuredRecord change, List<Schema.Field> fieldNames,
                                 int i) throws SQLException {
    for (Schema.Field field : fieldNames) {
      addValuesInPreparedStatement(stmt, getNonNullableType(field), i, change.get(field.getName()));
      i++;
    }
    return i;
  }

  private void fillInPrimaryKeyValues(PreparedStatement stmt, List<String> primaryKeys, StructuredRecord change, int i)
    throws SQLException {
    for (String field : primaryKeys) {
      addValuesInPreparedStatement(stmt, getNonNullableType(change.getSchema().getField(field)), i, change.get(field));
      i++;
    }
  }

  private void addValuesInPreparedStatement(PreparedStatement stmt, Schema.Type fieldType, int sqlIndex,
                                            Object fieldValue) throws SQLException {

    switch (fieldType) {
      case STRING:
        if (fieldValue == null) {
          stmt.setString(sqlIndex, null);
          return;
        }
        stmt.setString(sqlIndex, (String) fieldValue);
        break;
      case BOOLEAN:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.BIT);
          return;
        }
        stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
        break;
      case INT:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.INTEGER);
          return;
        }
        stmt.setInt(sqlIndex, (Integer) fieldValue);
        break;
      case LONG:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.BIGINT);
          return;
        }
        stmt.setLong(sqlIndex, (Long) fieldValue);
        break;
      case FLOAT:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.REAL);
          return;
        }
        stmt.setFloat(sqlIndex, (Float) fieldValue);
        break;
      case DOUBLE:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.FLOAT);
          return;
        }
        stmt.setDouble(sqlIndex, (Double) fieldValue);
        break;
      case BYTES:
        if (fieldValue == null) {
          stmt.setNull(sqlIndex, Types.BINARY);
          return;
        }
        stmt.setBytes(sqlIndex, (byte[]) fieldValue);
        break;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  private Schema.Type getNonNullableType(Schema.Field field) {
    Schema.Type type;
    if (field.getSchema().isNullable()) {
      type = field.getSchema().getNonNullable().getType();
    } else {
      type = field.getSchema().getType();
    }
    Preconditions.checkArgument(type.isSimpleType(),
                                "Only simple types are supported (boolean, int, long, float, double, string, bytes) " +
                                  "for writing a DBRecord, but found '%s' as the type for column '%s'. Please " +
                                  "remove this column or transform it to a simple type.", type, field.getName());
    return type;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
