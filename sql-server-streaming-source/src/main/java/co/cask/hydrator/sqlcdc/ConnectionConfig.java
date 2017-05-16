package co.cask.hydrator.sqlcdc;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link SQLServerStreamingSource}.
 */
public class ConnectionConfig extends PluginConfig {
  public static final String HOST_NAME = "hostname";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String DATABASE_NAME = "dbname";
  public static final String TABLE_NAME = "tableName";

  @Name(HOST_NAME)
  @Description("SQL Server hostname")
  @Macro
  public String hostname;

  @Name(PORT)
  @Description("SQL Server port")
  @Macro
  public String port;

  @Name(DATABASE_NAME)
  @Description("SQL Server database name. Note: CDC must be enabled on the database for change tracking")
  @Macro
  public String dbName;

  @Name(USERNAME)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String username;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  @Name(TABLE_NAME)
  @Description("SQL Server table name. Note: CDC must be enabled on the table for change tracking")
  @Macro
  public String tableName;
}
