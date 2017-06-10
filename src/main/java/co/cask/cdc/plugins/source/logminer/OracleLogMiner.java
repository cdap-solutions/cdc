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
import scala.reflect.ClassTag;

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
    Connection connection = null;
    try {
      // https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides
      // Follow the instructions in the above link to find out how to include Oracle JDBC driver.
      Class.forName(OracleDriver.class.getName());
      if (conf.username != null && conf.password != null) {
        connection = DriverManager.getConnection(getConnectionString(), conf.username, conf.password);
      } else {
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }

      // get change information dtream. This dstream has both schema and data changes
      LOG.info("Creating change information dstream");
      ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);
      JavaDStream<StructuredRecord> changeDStream =
        JavaDStream.fromDStream(new ChnageInputDStream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                       getConnectionString(), conf.username, conf
                                                         .password), tag);


      JavaDStream<Long> count = changeDStream.count();
      System.out.println("The count is: " + count);
      return changeDStream;
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private String getConnectionString() {
    return String.format("jdbc:oracle:thin:@%s:%d:%s", conf.hostName, conf.port, conf.dbName);
  }

}
