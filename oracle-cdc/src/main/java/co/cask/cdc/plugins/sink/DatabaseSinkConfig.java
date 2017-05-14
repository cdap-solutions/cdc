package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Database plugin config
 */
public class DatabaseSinkConfig extends ReferencePluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String TABLE_NAME = "tableName";
  public static final String QUERY = "query";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String SCHEMA = "outputschema";

  public DatabaseSinkConfig(String referenceName) {
    super(referenceName);
  }

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name.")
  @Macro
  public String connectionString;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  @Name(TABLE_NAME)
  @Description("Name of the database table to write to.")
  @Macro
  public String tableName;

  @Name(QUERY)
  @Description("Create table query. Please specify it if the table does not exist")
  @Nullable
  public String query;

  @Name(SCHEMA)
  @Description("Output schema for the table.")
  @Macro
  public String outputschema;

  public Schema getSchema() {
    try {
      return Schema.parseJson(outputschema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }
}
