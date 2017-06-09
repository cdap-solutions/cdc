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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdc.plugins.source.ReferenceStreamingSource;
import oracle.jdbc.driver.OracleDriver;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("OracleLogMiner")
@Description("Oracle LogMiner CDC Streaming Source")
public class OracleLogMiner extends ReferenceStreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(OracleLogMiner.class);
  private final ConnectionConfig conf;

  public OracleLogMiner(ConnectionConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    Connection connection;
    try {
      // https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides
      // Follow the instructions in the above link to find out how to include Oracle JDBC driver.
      Class.forName(OracleDriver.class.getName());
      if (conf.username != null && conf.password != null) {
        connection = DriverManager.getConnection(getConnectionString(), conf.username, conf.password);
      } else {
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    }

    setUpLogMiner(connection);
    queryLogMinerViewContent(connection);
    return null;
  }

  private void queryLogMinerViewContent(Connection connection) {
    // Set the given SCN or find out the last one used or get the latest one.
    // SELECT CURRENT_SCN FROM V$DATABASE; --> to get the latest one

    // Now query the LogMiner contents
    // select operation, table_name, sql_redo from v$logmnr_contents WHERE table_space = 'USERS'
    // AND scn >= $(scn) order by scn asc

    // Then filter the records by the right table : table_name from this column
    // Get the SQL query from the sql_redo column and use the PL/SQL Parser to parse the sql string and get the columns.

    // Persist the last SCN in our StateStore so that we can query from that point onwards in our next run.
  }

  private String getConnectionString() {
    return String.format("jdbc:oracle:thin:@%s:%d:%s", conf.hostName, conf.port, conf.dbName);
  }

  private void setUpLogMiner(Connection connection) {
    // TODO: Fetch all the redo logs using the following query
    // SELECT distinct member LOGFILENAME FROM V$LOGFILE;

    // TODO : Execute the below statement for all the files received from the above statement
    // execute DBMS_LOGMNR.ADD_LOGFILE ('/u01/app/oracle/oradata/xe/redo01.log');

    // TODO : Execute this statement to start LogMiner
    // execute DBMS_LOGMNR.START_LOGMNR (options => dbms_logmnr.dict_from_online_catalog);
  }
}
