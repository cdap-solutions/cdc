/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdc.plugins.sink.table;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.common.BatchReadableWritableConfig;
import co.cask.hydrator.plugin.common.Properties;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Plugin {@link Config} for Apache HBase.
 */
public class TableSinkConfig extends BatchReadableWritableConfig {

  @Name(Properties.Table.PROPERTY_SCHEMA)
  @Description("schema of the table as a JSON Object. If the table does not already exist, one will be " +
    "created with this schema, which will allow the table to be explored through Hive. If no schema is given, the " +
    "table created will not be explorable.")
  @Nullable
  private String schemaStr;

  @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
  @Description("The name of the record field that should be used as the row key when writing to the table.")
  private String rowField;

  public TableSinkConfig(String name, String rowField, @Nullable String schemaStr) {
    super(name);
    this.rowField = rowField;
    this.schemaStr = schemaStr;
  }

  // additional members required for CDC
  @Name("beforeField")
  @Description("Field in the Schema that corresponds to the fields to be discarded.")
  private String beforeField;

  @Name("afterField")
  @Description("Field in the Schema that corresponds to the fields to be inserted.")
  private String afterField;

  @Name("opTypeField")
  @Description("Field in the Schema that corresponds to the type of operation." +
    "\"I\" for insert. \"U\" for update. \"D\" for delete.")
  private String opTypeField;

  // methods for getting private members
  @Nullable
  public String getSchemaStr() {
    return schemaStr;
  }

  public String getRowField() {
    return rowField;
  }

  public String getOpTypeField() {return opTypeField;}

  public String getBeforeField() {
    return beforeField;
  }

  public String getAfterField() {return afterField;}

  public Schema getSchema() {
    try {
      return Schema.parseJson(schemaStr);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }
}
