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

package co.cask.cdc.plugins.source.logminer;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link OracleLogMiner}.
 */
public class ConnectionConfig extends ReferencePluginConfig {
  public static final String HOST_NAME = "hostname";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String DATABASE_NAME = "dbname";
  public static final String START_SCN = "startSCN";
  public static final String TABLE_NAMES = "tableNames";

  @Name(HOST_NAME)
  @Description("Oracle Server Host Name.")
  @Macro
  public String hostName;

  @Name(PORT)
  @Description("Oracle Server Port. Defaults to 1521.")
  public int port;

  @Name(DATABASE_NAME)
  @Description("Oracle Database Name.")
  @Macro
  public String dbName;

  @Name(USERNAME)
  @Description("User to use to connect to the specified DB.")
  @Nullable
  @Macro
  public String username;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified.")
  @Nullable
  @Macro
  public String password;

  @Name(TABLE_NAMES)
  @Description("A comma separated list of Table Names for which CDC needs to be captured.")
  @Nullable
  @Macro
  public String tableNames;

  @Name(START_SCN)
  @Description("SCN to start from. Defaults to the latest SCN.")
  @Nullable
  @Macro
  public long startSCN;

  public ConnectionConfig() {
    super("");
    port = 1521;
    username = null;
    password = null;
  }
}
