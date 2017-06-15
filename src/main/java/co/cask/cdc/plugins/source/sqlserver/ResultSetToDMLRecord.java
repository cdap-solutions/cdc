package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdc.plugins.common.Constants;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  private static final int CHANGE_TABLE_COLUMNS_SIZE = 3;
  private final TableInformation tableInformation;


  ResultSetToDMLRecord(TableInformation tableInformation) {
    this.tableInformation = tableInformation;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws SQLException {
    Schema changeSchema = getChangeSchema(resultSet);
    Schema dmlSchema = getDMLSchema(changeSchema);

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(dmlSchema);
    recordBuilder.set(Constants.DMLRecord.TABLE_SCHEMA_FIELD.getName(), Joiner.on(".").join(tableInformation
                                                                                              .getSchemaName(),
                                                                                            tableInformation.getName()));
    recordBuilder.set(Constants.DMLRecord.PRIMARY_KEYS_SCHEMA_FIELD.getName(), Lists.newArrayList(tableInformation.getPrimaryKeys()));
    recordBuilder.set(Constants.DMLRecord.OP_TYPE_SCHEMA_FIELD.getName(), resultSet.getString("SYS_CHANGE_OPERATION"));
    return getChangeData(resultSet, changeSchema, recordBuilder);
  }

  private StructuredRecord getChangeData(ResultSet resultSet, Schema changeSchema,
                                         StructuredRecord.Builder recordBuilder) throws SQLException {
    StructuredRecord.Builder changeRecordBuilder = StructuredRecord.builder(changeSchema);
    ResultSetMetaData metadata = resultSet.getMetaData();
    for (int i = 0; i < changeSchema.getFields().size(); i++) {
      int sqlColumnType = metadata.getColumnType(i + CHANGE_TABLE_COLUMNS_SIZE);
      Schema.Field field = changeSchema.getFields().get(i);
      changeRecordBuilder.set(field.getName(), DBUtils.transformValue(sqlColumnType, resultSet, field.getName()));
    }
    StructuredRecord changeRecord = changeRecordBuilder.build();
    recordBuilder.set("change", changeRecord);
    return recordBuilder.build();
  }

  private Schema getDMLSchema(Schema changeSchema) {
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(Constants.DMLRecord.TABLE_SCHEMA_FIELD);
    schemaFields.add(Constants.DMLRecord.PRIMARY_KEYS_SCHEMA_FIELD);
    schemaFields.add(Constants.DMLRecord.OP_TYPE_SCHEMA_FIELD);
    schemaFields.add(Schema.Field.of("change", changeSchema));
    return Schema.recordOf(Constants.DMLRecord.RECORD_NAME, schemaFields);
  }

  private Schema getChangeSchema(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(resultSet);
    // drop first three columns as they are from change tracking tables and does not represent the change data
    return Schema.recordOf("rec", schemaFields.subList(CHANGE_TABLE_COLUMNS_SIZE, schemaFields.size()));

  }
}
