package co.cask.cdc.plugins.common;

import co.cask.cdap.api.data.schema.Schema;

/**
 * Created by rsinha on 6/15/17.
 */
public class Constants {

  public static class DMLRecord {
    public static final Schema.Field TABLE_SCHEMA_FIELD = Schema.Field.of("table", Schema.of(Schema.Type.STRING));
    public static final Schema.Field PRIMARY_KEYS_SCHEMA_FIELD = Schema.Field.of("primary_keys", Schema.arrayOf
      (Schema.of(Schema.Type.STRING)));
    public static final Schema.Field OP_TYPE_SCHEMA_FIELD = Schema.Field.of("op_type", Schema.of(Schema.Type.STRING));

    public static final String RECORD_NAME = "DMLRecord";
  }

  public static class DDLRecord {
    public static final String RECORD_NAME = "DDLRecord";
    public static final Schema DDL_SCHEMA = Schema.recordOf(RECORD_NAME,
                                                             Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("schema", Schema.of(Schema.Type.STRING)));
  }
}
