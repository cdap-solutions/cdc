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

package co.cask.cdc.plugins.source.logminer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import oracle.jdbc.rowset.OracleSerialBlob;
import oracle.jdbc.rowset.OracleSerialClob;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {
  private static final Schema.Field TABLE_FIELD = Schema.Field.of("table", Schema.of(Schema.Type.STRING));
  private static final Schema.Field PRIMARY_KEYS_FIELD = Schema.Field.of("primary_keys", Schema.arrayOf(Schema.of(Schema.Type.STRING)));
  private static final Schema.Field OP_TYPE_FIELD = Schema.Field.of("op_type", Schema.of(Schema.Type.STRING));

  private final String tableName;
  private final List<String> primaryKeys;
  private final Map<String, Integer> fieldTypes;
  private final List<Schema.Field> fieldList;

  static final String RECORD_NAME = "DMLRecord";
  private final SQLParser sqlParser = new SQLParser();

  public ResultSetToDMLRecord(String tableName, List<String> primaryKeys,
                              Map<String, Integer> fieldTypes, List<Schema.Field> fieldList) {
    this.tableName = tableName;
    this.primaryKeys = primaryKeys;
    this.fieldTypes = fieldTypes;
    this.fieldList = fieldList;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws Exception {
    Schema changeSchema = getChangeSchema();
    Schema dmlSchema = getDMLSchema();

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(dmlSchema);
    recordBuilder.set(TABLE_FIELD.getName(), tableName);
    recordBuilder.set(PRIMARY_KEYS_FIELD.getName(), primaryKeys);
    recordBuilder.set(OP_TYPE_FIELD.getName(), getOpType(resultSet.getString("OPERATION")));
    return getChangeData(resultSet, changeSchema, recordBuilder);
  }

  private String getOpType(String fromDB) {
    if (fromDB.equalsIgnoreCase("INSERT")) {
      return "I";
    }

    if (fromDB.equalsIgnoreCase("UPDATE")) {
      return "U";
    }

    if (fromDB.equalsIgnoreCase("DELETE")) {
      return "D";
    }
    return "UNKNOWN";
  }

  private StructuredRecord getChangeData(ResultSet resultSet, Schema changeSchema,
                                         StructuredRecord.Builder recordBuilder) throws Exception {
    StructuredRecord.Builder changeRecordBuilder = StructuredRecord.builder(changeSchema);

    String sql_redo = resultSet.getString("SQL_REDO");
    Map<String, String> dataFields = sqlParser.parseSQL(sql_redo);

    for (Map.Entry<String, Integer> fieldType : fieldTypes.entrySet()) {
      String fieldName = fieldType.getKey();
      Integer sqlType = fieldType.getValue();
      changeRecordBuilder.set(fieldName, transformValue(sqlType, dataFields.get(fieldName)));
    }
    StructuredRecord changeRecord = changeRecordBuilder.build();
    recordBuilder.set("change", changeRecord);
    return recordBuilder.build();
  }

  private Object transformValue(Integer sqlColumnType, String original) throws SQLException {
    switch (sqlColumnType) {
      case Types.SMALLINT:
      case Types.TINYINT:
        return Integer.valueOf(original);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return Double.valueOf(original);
      case Types.DATE:
        return Long.valueOf(original);
      case Types.TIME:
        return Long.valueOf(original);
      case Types.TIMESTAMP:
        return Long.valueOf(original);
      case Types.BLOB:
        Blob blob = new OracleSerialBlob(original.getBytes());
        return blob.getBytes(1, (int) blob.length());
      case Types.CLOB:
        Clob oracleClob = new OracleSerialClob(original.toCharArray());
        return oracleClob.getSubString(1, (int) oracleClob.length());
      default:
        return null;
    }
  }

  private Schema getDMLSchema() {
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(TABLE_FIELD);
    schemaFields.add(PRIMARY_KEYS_FIELD);
    schemaFields.add(OP_TYPE_FIELD);
    schemaFields.add(Schema.Field.of("change", getChangeSchema()));
    return Schema.recordOf(RECORD_NAME, schemaFields);
  }

  private Schema getChangeSchema() {
    return Schema.recordOf("rec", fieldList);
  }

}