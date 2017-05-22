package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Normalizer for SQL Server CDC.
 */
@Plugin(type = "transform")
@Name("SQLServerCDCNormalizer")
public class Normalizer extends Transform<StructuredRecord, StructuredRecord> {
  private static Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static Gson GSON = new Gson();

  private static Set<String> DDL_SYSTEM_FIELD = Sets.newHashSet("__$start_lsn", "__$seqval", "__$operation",
                                                                "__$update_mask");

  private static Schema.Field TABLE_NAME_SCHEMA_FIELD = Schema.Field.of("tablename", Schema.of(Schema.Type.STRING));
  private static Schema.Field OP_TYPE_SCHEMA_FIELD = Schema.Field.of("op_type", Schema.of(Schema.Type.STRING));


  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("Input StructuredRecord is {}", GSON.toJson(input));
    LOG.info("The op type is {}", input.get("__$operation"));
    int operation = input.get("__$operation");
    StructuredRecord.Builder recordBuilder;
    StructuredRecord record;
    switch (operation) {
      case 2:
        // insert
        recordBuilder = getInsertRecord(input);
        record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "I").build();
        LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
        emitter.emit(record);
        break;
      case 3:
        // no-op
        // We will never see case operation 3 which represent the old record for update (operation 4)
        // Note: The old record is also not needed if an update happened changing the index column
        // because for such change the cdc has a delete and an insert not an update.
        LOG.info("Found change data entry with operation type {}. Ignoring it as old record during update is not " +
                   "used. Will process update later when we find Operation type 4.", operation);
        break;
      case 4:
        // update
        recordBuilder = getInsertRecord(input);
        record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "U").build();
        LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
        emitter.emit(record);
        break;
      case 1:
        // delete
        recordBuilder = getInsertRecord(input);
        record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "D").build();
        LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
        emitter.emit(record);
      default:
        throw new IllegalArgumentException("Unknown type" + operation);

    }
  }

  private StructuredRecord.Builder getInsertRecord(StructuredRecord input) {
    List<Schema.Field> ddlFields = new LinkedList<>();
    Schema schema = input.getSchema();
    for (Schema.Field field : schema.getFields()) {
      if (DDL_SYSTEM_FIELD.contains(field.getName().toLowerCase())) {
        continue;
      }
      ddlFields.add(field);
    }
    Schema ddlDataSchema = Schema.recordOf("after", ddlFields);

    StructuredRecord.Builder innerRecordBuilder = StructuredRecord.builder(ddlDataSchema);
    for (Schema.Field field : ddlDataSchema.getFields()) {
      innerRecordBuilder.set(field.getName(), input.get(field.getName()));
    }

    Schema cdcSchema = Schema.recordOf("cdc", TABLE_NAME_SCHEMA_FIELD, OP_TYPE_SCHEMA_FIELD, Schema.Field.of
      ("innerRecord", ddlDataSchema));


    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(cdcSchema);
    recordBuilder
      .set(TABLE_NAME_SCHEMA_FIELD.getName(), "singleTable")

      .set("innerRecord", innerRecordBuilder.build()).build();
    return recordBuilder;
  }
}
