package co.cask.cdc.plugins.sink;

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
    String operationType = record.get("op_type");
    switch (operationType) {
      case "I":
        StructuredRecord insertRecord = record.get("after");
        int i = 1;
        for (Schema.Field field : insertRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = insertRecord.get(fieldName);
            addValuesInDataOutput(out, fieldType, fieldValue);
            i++;
          }
        }
        break;
      case "U":
        StructuredRecord updateRecord = record.get("after");
        i = 1;
        for (Schema.Field field : updateRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = updateRecord.get(fieldName);
            addValuesInDataOutput(out, fieldType, fieldValue);
            i++;
          }
        }
        break;
      case "D":
        StructuredRecord deleteRecord = record.get("before");
        i = 1;
        for (Schema.Field field : deleteRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = deleteRecord.get(fieldName);
            addValuesInDataOutput(out, fieldType, fieldValue);
            i++;
          }
        }
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);

    }
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    String operationType = record.get("op_type");
    switch (operationType) {
      case "I":
        StructuredRecord insertRecord = record.get("after");
        int i = 1;
        for (Schema.Field field : insertRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = insertRecord.get(fieldName);
            addValuesInPreparedStatement(stmt, fieldType, i, fieldValue);
            i++;
          }
        }
        break;
      case "U":
        StructuredRecord updateRecordAfter = record.get("after");
        StructuredRecord updateRecordBefore = record.get("before");
        i = 1;
        // fill in updated values
        for (Schema.Field field : updateRecordAfter.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = updateRecordAfter.get(fieldName);
            addValuesInPreparedStatement(stmt, fieldType, i, fieldValue);
            i++;
          }
        }

        // fill in non-updated values
        for (Schema.Field field : updateRecordBefore.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = updateRecordBefore.get(fieldName);
            addValuesInPreparedStatement(stmt, fieldType, i, fieldValue);
            i++;
          }
        }
        break;
      case "D":
        StructuredRecord deleteRecord = record.get("before");
        i = 1;
        for (Schema.Field field : deleteRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            Schema.Type fieldType = getNonNullableType(field);
            Object fieldValue = deleteRecord.get(fieldName);
            addValuesInPreparedStatement(stmt, fieldType, i, fieldValue);
            i++;
          }
        }
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);

    }
  }

  private void addValuesInPreparedStatement(PreparedStatement stmt, Schema.Type fieldType, int sqlIndex,
                                            Object fieldValue) throws SQLException {

    switch (fieldType) {
      case STRING:
        // clob can also be written to as setString
        stmt.setString(sqlIndex, (String) fieldValue);
        break;
      case BOOLEAN:
        stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
        break;
      case INT:
        stmt.setInt(sqlIndex, (Integer) fieldValue);
        break;
      case LONG:
        stmt.setLong(sqlIndex, (Long) fieldValue);
        break;
      case FLOAT:
        stmt.setFloat(sqlIndex, (Float) fieldValue);
        break;
      case DOUBLE:
        stmt.setDouble(sqlIndex, (Double) fieldValue);
        break;
      case BYTES:
        stmt.setBytes(sqlIndex, (byte[]) fieldValue);
        break;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  private void addValuesInDataOutput(DataOutput out, Schema.Type fieldType, Object fieldValue)  throws IOException {

    switch (fieldType) {
      case STRING:
        // write string appropriately
        out.writeUTF((String) fieldValue);
        break;
      case BOOLEAN:
        out.writeBoolean((Boolean) fieldValue);
        break;
      case INT:
        out.writeInt((Integer) fieldValue);
        break;
      case LONG:
        out.writeLong((Long) fieldValue);
        break;
      case FLOAT:
        out.writeFloat((Float) fieldValue);
        break;
      case DOUBLE:
        out.writeDouble((Double) fieldValue);
        break;
      case BYTES:
        out.write((byte[]) fieldValue);
        break;
      default:
        throw new IOException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
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
