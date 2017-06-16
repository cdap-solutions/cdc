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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdc.plugins.source.sqlserver.ResultSetToDDLRecord;
import com.google.common.base.Throwables;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A {@link InputDStream} which reads chnage tracking data from SQL Server and emits {@link StructuredRecord}
 */
public class ChangeInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeInputDStream.class);
  private ClassTag<StructuredRecord> tag;
  private String connectionUrl;
  private String username;
  private String password;
  private Set<String> trackedTables;
  //  private OracleServerConnection dbConnection;
  private long commitSCN;

  ChangeInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connectionUrl, String username,
                     String password, Set<String> trackedTables, long commitSCN) {
    super(ssc, tag);
    this.tag = tag;
    this.connectionUrl = connectionUrl;
    this.username = username;
    this.password = password;
    this.trackedTables = trackedTables;
    this.commitSCN = commitSCN;
  }

  ChangeInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connectionUrl, String username,
                     String password, Set<String> trackedTables) {
    this(ssc, tag, connectionUrl, username, password, trackedTables, 0);
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {

    // get the table information of all tables which have ct enabled.

    List<RDD<StructuredRecord>> changeRDDs = new LinkedList<>();

    // Get the schema of tables. We get the schema of tables every microbatch because we want to update  the downstream
    // dataset with the DDL changes if any.
    for (String tableInformation : trackedTables) {
      changeRDDs.add(getColumnns(tableInformation));
    }

    try {
      long prev = commitSCN;
      OracleServerConnection connection = new OracleServerConnection(this.connectionUrl, username, password, true);
      long cur = getCurrentCommitSCN(connection);
      if (prev != cur) {
        JdbcRDD<StructuredRecord> rdd = queryLogMinerViewContent(prev, cur);
        changeRDDs.add(rdd);
        commitSCN = cur;
      } else {
        changeRDDs.add(ssc().sc().emptyRDD(tag));
      }
//      List<String> primaryKeys = getPrimaryKeys(trackedTables, dbConnection);
//      List<Schema.Field> fieldList = getFieldList(trackedTables, dbConnection);

//      return Option.apply(changes.toJavaRDD().rdd());


      // update the tracking version
      RDD<StructuredRecord> changes = ssc().sc().union(JavaConversions.asScalaBuffer(changeRDDs), tag);
      return Option.apply(changes);
    } catch (SQLException e) {
      e.printStackTrace();
      throw Throwables.propagate(e);
    }
  }

  private long getCurrentCommitSCN(OracleServerConnection dbConnection) throws SQLException {
    Connection connection = dbConnection.apply();
    ResultSet resultSet = connection.createStatement().executeQuery("select MAX(COMMIT_SCN) from v$logmnr_contents WHERE table_space = 'USERS'");
    long changeVersion = 0;
    while (resultSet.next()) {
      changeVersion = resultSet.getLong(1);
      LOG.info("Current commitSCN is {}", changeVersion);
    }
    connection.close();
    return changeVersion;
  }

  @Override
  public void start() {
//    dbConnection = new OracleServerConnection(connectionUrl, username, password);
  }

  @Override
  public void stop() {
//    try {
//      closeLogMiner(dbConnection.apply());
//    } catch (SQLException e) {
//      e.printStackTrace();
//    }
    // no-op
    // Also no need to close the dbconnection as JdbcRDD takes care of closing it
  }


  private JdbcRDD<StructuredRecord> queryLogMinerViewContent(long prev, long cur) throws SQLException {

    String stmt = String.format("select operation, table_name, sql_redo from v$logmnr_contents WHERE table_space = " +
                                  "'USERS' AND %s AND COMMIT_SCN > %s AND COMMIT_SCN <= %s AND ?=?",
                                getTableNameQuery(), prev, cur);
    LOG.info("Querying for change data with statement {}", stmt);

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(ssc().sc(), new OracleServerConnection(connectionUrl, username, password, true), stmt, 1, 1, 1,
                         new ResultSetToDMLRecord(connectionUrl, username, password),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
    // Set the given SCN or find out the last one used or get the latest one.
    // SELECT CURRENT_SCN FROM V$DATABASE; --> to get the latest one

    // Now query the LogMiner contents
    // select operation, table_name, sql_redo from v$logmnr_contents WHERE table_space = 'USERS'
    // AND scn >= $(scn) order by scn asc

    // Then filter the records by the right table : table_name from this column
    // Get the SQL query from the sql_redo column and use the PL/SQL Parser to parse the sql string and get the columns.

    // Persist the last SCN in our StateStore so that we can query from that point onwards in our next run.
  }

  private JdbcRDD<StructuredRecord> getColumnns(String tableName) {
    String stmt = String.format("SELECT * FROM %s WHERE ROWNUM = 1 AND ?=?", tableName);
    LOG.info("Querying with {}", stmt);

    return new JdbcRDD<>(ssc().sc(), new OracleServerConnection(connectionUrl, username, password), stmt, 1, 1, 1,
                         new ResultSetToDDLRecord("USER", tableName),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
  }

  private String getTableNameQuery() {
    StringBuilder queryBuilder = new StringBuilder();
    for (String table : trackedTables) {

      if (queryBuilder.length() != 0) {
        queryBuilder.append(", ");
      }
      queryBuilder.append("'").append(table).append("'");
    }

    return "TABLE_NAME in (" + queryBuilder.toString() + ")";
  }
}
