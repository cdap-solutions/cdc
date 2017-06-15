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
import co.cask.hydrator.plugin.DBUtils;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {
  private static final Schema.Field TABLE_FIELD = Schema.Field.of("table", Schema.of(Schema.Type.STRING));
  private static final Schema.Field PRIMARY_KEYS_FIELD = Schema.Field.of("primary_keys", Schema.arrayOf(Schema.of(Schema.Type.STRING)));
  private static final Schema.Field OP_TYPE_FIELD = Schema.Field.of("op_type", Schema.of(Schema.Type.STRING));

  private final OracleServerConnection dbConnection;

  public static final String RECORD_NAME = "DMLRecord";
  private final SQLParser sqlParser = new SQLParser();

  public ResultSetToDMLRecord(OracleServerConnection dbConnection) {
    this.dbConnection = dbConnection;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws Exception {
    String tableName = resultSet.getString("TABLE_NAME");
    Schema changeSchema = getChangeSchema(tableName);
    Schema dmlSchema = getDMLSchema(tableName);

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(dmlSchema);
    // TODO: sink expects schema.tablename

    List<String> primaryKeys = getPrimaryKeys(tableName);
    recordBuilder.set(TABLE_FIELD.getName(), "USER." + tableName);
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

    for (int i = 0; i < changeSchema.getFields().size(); i++) {
      String fieldName = changeSchema.getFields().get(i).getName();
      // all the values have '' so get rid of them
      // TODO: Convert and set will not support BLOB and CLOB. Change this to the one in Databaset
      changeRecordBuilder.convertAndSet(fieldName, dataFields.get(fieldName).replaceAll("^'|'$", ""));
    }
    StructuredRecord changeRecord = changeRecordBuilder.build();
    recordBuilder.set("change", changeRecord);
    return recordBuilder.build();
  }

  private Schema getDMLSchema(String tableName) throws SQLException {
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(TABLE_FIELD);
    schemaFields.add(PRIMARY_KEYS_FIELD);
    schemaFields.add(OP_TYPE_FIELD);
    schemaFields.add(Schema.Field.of("change", getChangeSchema(tableName)));
    return Schema.recordOf(RECORD_NAME, schemaFields);
  }

  private Schema getChangeSchema(String tableName) throws SQLException {
    return Schema.recordOf("rec", getFieldList(tableName));
  }

  private List<Schema.Field> getFieldList(String tableName) throws SQLException {
    try (Connection connection = dbConnection.apply()) {
      ResultSet resultSet = connection.createStatement().executeQuery(String.format(
        "SELECT * FROM %s WHERE 1 = 0", tableName));
      return DBUtils.getSchemaFields(resultSet);
    }
  }

  // DO not remove this as its needed to get BLOB and other complex field types
  private Map<String, Integer> getTableFields(String tableName, OracleServerConnection dbConnection) throws SQLException {
    try (Connection connection = dbConnection.apply()) {
      ResultSet resultSet = connection.createStatement().executeQuery(String.format(
        "SELECT * FROM %s WHERE 1 = 0", tableName));
      Map<String, Integer> fieldTypes = new HashMap<>();
      int columnCount = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String name = resultSet.getMetaData().getColumnLabel(i);
        int type = resultSet.getMetaData().getColumnType(i);
        fieldTypes.put(name, type);
      }
      return fieldTypes;
    }
  }

  private List<String> getPrimaryKeys(String tableName) throws SQLException {
    try (Connection connection = dbConnection.apply()) {
      List<String> keys = new ArrayList<>();
      ResultSet resultSet = connection.createStatement().executeQuery(String.format(
        "SELECT column_name FROM all_cons_columns WHERE constraint_name = (" +
          "SELECT constraint_name FROM user_constraints WHERE UPPER(table_name) = UPPER('%s') " +
          "AND CONSTRAINT_TYPE = 'P')", tableName));
      while (resultSet.next()) {
        keys.add(resultSet.getString(1));
      }
      return keys;
    }
  }
}
