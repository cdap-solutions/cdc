package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord}
 */
public class ResultSetToDMLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  TableInformation tableInformation;

  public ResultSetToDMLRecord(TableInformation tableInformation) {
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
    ResultSetMetaData metadata = resultSet.getMetaData();
    // Nullable schema because for delete we will not have data in table even though the columns are non-nullable
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(Schema.Field.of("table", Schema.of(Schema.Type.STRING)));
    schemaFields.add(Schema.Field.of("primary_keys", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    Schema changeSchema = Schema.recordOf("rec", DBUtils.getSchemaFields(resultSet));
    schemaFields.add(Schema.Field.of("change", changeSchema));
    Schema schema = Schema.recordOf("DMLRecord", schemaFields);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    // 0 th field is tableName
    recordBuilder.set("table", Joiner.on(".").join(tableInformation.getSchemaName(), tableInformation.getName()));

    recordBuilder.set("primary_keys", Lists.newArrayList(tableInformation.getPrimaryKeys()));
    StructuredRecord.Builder changeRecordBuilder = StructuredRecord.builder(changeSchema);
//    for (int i = 1; i <= metadata.getColumnCount(); i++) {
//      Schema.Field field = changeSchema.getFields().get(i - 1);
//      int sqlColumnType = metadata.getColumnType(i);
//      changeRecordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet, field.getName()));
//    }
    for (int i = 0; i < changeSchema.getFields().size() - 1; i++) {
      Schema.Field field = changeSchema.getFields().get(i);
      int sqlColumnType = metadata.getColumnType(i + 1);
      changeRecordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet, field.getName()));
    }
    StructuredRecord changeRecord = changeRecordBuilder.build();
    recordBuilder.set("change", changeRecord);
    return recordBuilder.build();
  }

  private static List<Schema.Field> getNullableSchema(List<Schema.Field> fields) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Schema schema = Schema.nullableOf(field.getSchema());
      schemaFields.add(Schema.Field.of(name, schema));
    }
    return schemaFields;
  }


  //TODO: This function is taken from DatabaseSource. We should move it to the DBUtil class in Datasbase plugin and
  // use it here.
  @Nullable
  static Object transformValue(int sqlColumnType, ResultSet resultSet, String fieldName) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlColumnType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          return ((BigDecimal) original).doubleValue();
        case Types.DATE:
          return resultSet.getDate(fieldName).getTime();
        case Types.TIME:
          return resultSet.getTime(fieldName).getTime();
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(fieldName).getTime();
        case Types.BLOB:
          Object toReturn;
          Blob blob = (Blob) original;
          try {
            toReturn = blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
          return toReturn;
        case Types.CLOB:
          String s;
          StringBuilder sbf = new StringBuilder();
          Clob clob = (Clob) original;
          try {
            try (BufferedReader br = new BufferedReader(clob.getCharacterStream(1, (int) clob.length()))) {
              if ((s = br.readLine()) != null) {
                sbf.append(s);
              }
              while ((s = br.readLine()) != null) {
                sbf.append(System.getProperty("line.separator"));
                sbf.append(s);
              }
            }
          } catch (IOException e) {
            throw new SQLException(e);
          } finally {
            clob.free();
          }
          return sbf.toString();
      }
    }
    return original;
  }
}
