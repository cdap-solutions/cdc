package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
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

  private static final Set<String> DDL_SYSTEM_FIELD = Sets.newHashSet("SYS_CHANGE_CREATION_VERSION",
                                                                      "SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION");

  private static final Schema.Field TABLE_NAME_SCHEMA_FIELD = Schema.Field.of("tablename", Schema.of(Schema.Type
                                                                                                       .STRING));
  private static final Schema.Field OP_TYPE_SCHEMA_FIELD = Schema.Field.of("op_type", Schema.of(Schema.Type.STRING));

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    LOG.info("--------------------------------------------------------------------");
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("Input StructuredRecord is {}", GSON.toJson(input));
    LOG.info("The op type is {}", input.get("SYS_CHANGE_OPERATION"));
    String operation = input.get("SYS_CHANGE_OPERATION");
    StructuredRecord.Builder recordBuilder;
    StructuredRecord record;
    if (operation.equalsIgnoreCase("D")) {
      // delete
      recordBuilder = getInsertRecord(input);
      record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "D").build();
      LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
      emitter.emit(record);
    } else if (operation.equalsIgnoreCase("I")) {
      // insert
      recordBuilder = getInsertRecord(input);
      record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "I").build();
      LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
      emitter.emit(record);
    } else if (operation.equalsIgnoreCase("U")) {
      // update
      recordBuilder = getInsertRecord(input);
      record = recordBuilder.set(OP_TYPE_SCHEMA_FIELD.getName(), "U").build();
      LOG.info("Output StructuredRecord is {}", GSON.toJson(record));
      emitter.emit(record);
    } else {
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
      .set(TABLE_NAME_SCHEMA_FIELD.getName(), input.get("tableName"))
      .set("innerRecord", innerRecordBuilder.build());
    return recordBuilder;
  }
}
