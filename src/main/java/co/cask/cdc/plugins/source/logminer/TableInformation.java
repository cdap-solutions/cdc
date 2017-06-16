package co.cask.cdc.plugins.source.logminer;

import co.cask.cdap.api.data.schema.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by rsinha on 6/16/17.
 */
public class TableInformation implements Serializable {

  private final Map<String, Integer> tableFields;
  private final List<String> primaryKeys;
  private final List<Schema.Field> schemaFields;


  public TableInformation(Map<String, Integer> tableFields, List<String> primaryKeys, List<Schema.Field> schemaFields) {
    this.tableFields = tableFields;
    this.primaryKeys = primaryKeys;
    this.schemaFields = schemaFields;
  }

  public Map<String, Integer> getTableFields() {
    return tableFields;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public List<Schema.Field> getSchemaFields() {
    return schemaFields;
  }
}
