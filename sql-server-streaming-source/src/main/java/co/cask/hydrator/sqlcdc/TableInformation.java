package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import scala.Serializable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents SQL Server Table information
 */
class TableInformation implements Serializable {
  private final String schemaName;
  private final String name;
  private final List<Schema.Field> columnSchema;
  private final Set<String> primaryKeys;
  private final Set<Schema.Field> valueColumns;

  TableInformation(String schemaName, String name, List<Schema.Field> columnSchema, Set<String> primaryKeys) {
    this.schemaName = schemaName;
    this.name = name;
    this.columnSchema = columnSchema;
    this.primaryKeys = primaryKeys;
    this.valueColumns = ImmutableSet.copyOf(Sets.difference(Sets.<Schema.Field>newLinkedHashSet(columnSchema),
                                                            primaryKeys));
  }

  @Override
  public String toString() {
    return "TableInformation{" +
      "schemaName='" + schemaName + '\'' +
      ", name='" + name + '\'' +
      ", columnSchema=" + columnSchema +
      ", primaryKeys=" + primaryKeys +
      ", valueColumns=" + valueColumns +
      '}';
  }

  String getSchemaName() {
    return schemaName;
  }

  String getName() {
    return name;
  }

  List<Schema.Field> getColumnSchema() {
    return columnSchema;
  }

  Set<String> getPrimaryKeys() {
    return primaryKeys;
  }

  Set<Schema.Field> getValueColumns() {
    return valueColumns;
  }

  Set<String> getValueColumnNames() {
    Set<String> names = new LinkedHashSet<>();
    for (Schema.Field valueColumn : valueColumns) {
      names.add(valueColumn.getName());
    }
    return names;
  }

}
