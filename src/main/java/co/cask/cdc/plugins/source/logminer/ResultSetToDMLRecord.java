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
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
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
  static final String RECORD_NAME = "DMLRecord";
  private final SQLParser sqlParser = new SQLParser();

  public ResultSetToDMLRecord(String tableName, List<String> primaryKeys) {
    this.tableName = tableName;
    this.primaryKeys = primaryKeys;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws Exception {
    Schema changeSchema = Schema.recordOf(RECORD_NAME,
                                          Schema.Field.of("redoLog", Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(changeSchema);

    String sql_redo = resultSet.getString("SQL_REDO");

    Map<String, String> fields = sqlParser.parseSQL(sql_redo);


    // TODO: Map the sql query to  correct  structure record here
    System.out.printf("### The redo log is " + sql_redo);
    recordBuilder.set("redoLog", sql_redo);


    return recordBuilder.build();
  }

}
