package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
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
import java.util.List;
import javax.annotation.Nullable;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord}
 */
public class ResultSetToStructureRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(resultSet);
    Schema schema = Schema.recordOf("changeRecord", schemaFields);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (int i = 0; i < schemaFields.size() - 1; i++) {
      Schema.Field field = schemaFields.get(i);
      int sqlColumnType = metadata.getColumnType(i + 1);
      recordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet, field.getName()));
    }
    return recordBuilder.build();
  }

  //TODO: This function is taken from DatabaseSource. We should move it to the DBUtil class in Datasbase plugin and
  // use it here.
  @Nullable
  private Object transformValue(int sqlColumnType, ResultSet resultSet, String fieldName) throws SQLException {
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
