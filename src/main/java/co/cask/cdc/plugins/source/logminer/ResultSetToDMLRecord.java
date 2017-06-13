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
import co.cask.cdap.format.StructuredRecordStringConverter;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
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
  private final List<Schema.Field> fieldList;

  private static final String RECORD_NAME = "DMLRecord";
  private final SQLParser sqlParser = new SQLParser();

  ResultSetToDMLRecord(String tableName, List<String> primaryKeys, List<Schema.Field> fieldList) {
    this.tableName = tableName;
    this.primaryKeys = primaryKeys;
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
    // TODO: sink expects schema.tablename
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
