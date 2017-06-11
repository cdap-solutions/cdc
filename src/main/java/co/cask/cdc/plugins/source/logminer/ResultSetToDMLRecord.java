package co.cask.cdc.plugins.source.logminer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  static final String RECORD_NAME = "DMLRecord";


  public StructuredRecord apply(ResultSet row) {
    try {
      return resultSetToStructureRecord(row);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord resultSetToStructureRecord(ResultSet resultSet) throws SQLException {
    Schema changeSchema = Schema.recordOf(RECORD_NAME,
                                          Schema.Field.of("redoLog", Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(changeSchema);

    String sql_redo = resultSet.getString("SQL_REDO");
    // TODO: Map the sql query to  correct  structure record here
    System.out.printf("### The redo log is " + sql_redo);
    recordBuilder.set("redoLog", sql_redo);


    return recordBuilder.build();

  }

}
