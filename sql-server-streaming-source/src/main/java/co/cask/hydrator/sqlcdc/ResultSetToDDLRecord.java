package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for DDL.
 */
public class ResultSetToDDLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  private static final Schema DDL_SCHEMA = Schema.recordOf("DDLRecord",
                                                           Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("schema", Schema.of(Schema.Type.STRING)));

  private final String schemaName;
  private final String tableName;

  public ResultSetToDDLRecord(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return transform(row);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord transform(ResultSet resultSet) throws SQLException {
    Schema tableSchema = Schema.recordOf("schema", DBUtils.getSchemaFields(resultSet));
    System.out.println("### The tableschema for DDL is " + schemaName + " " + tableName);

    StructuredRecord.Builder builder = StructuredRecord.builder(DDL_SCHEMA);
    builder.set("table", schemaName + "." + tableName);
    builder.set("schema", tableSchema.toString());
    return builder.build();
  }
}
