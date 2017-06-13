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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Throwables;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link InputDStream} which reads chnage tracking data from SQL Server and emits {@link StructuredRecord}
 */
public class ChangeInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeInputDStream.class);
  private ClassTag<StructuredRecord> tag;
  private String connection;
  private String username;
  private String password;
  private String tableName;
  private OracleServerConnection dbConnection;
  private long scn;

  ChangeInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                     String password, String tableName, long scn) {
    super(ssc, tag);
    this.tag = tag;
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.tableName = tableName;
    this.scn = scn;
  }

  ChangeInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                     String password) {
    super(ssc, tag);
    this.tag = tag;
    this.connection = connection;
    this.username = username;
    this.password = password;
    // if not current tracking version is given initialize it to 0
    this.scn = 0;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {

    try {
      long prev = scn;
      long cur = getCurrentSCN(dbConnection);
      List<String> primaryKeys = getPrimaryKeys(tableName, dbConnection);
      List<Schema.Field> fieldList = getFieldList(tableName, dbConnection);
      JdbcRDD<StructuredRecord> changes = queryLogMinerViewContent(prev, cur, primaryKeys, fieldList);
      scn = cur;
      return Option.apply(changes.toJavaRDD().rdd());
    } catch (SQLException e) {
      e.printStackTrace();
      throw Throwables.propagate(e);
    }
  }

  private List<Schema.Field> getFieldList(String tableName, OracleServerConnection dbConnection) throws SQLException {
    Connection connection = dbConnection.apply();
    ResultSet resultSet = connection.createStatement().executeQuery(String.format(
      "SELECT * FROM %s WHERE 1 = 0", tableName));
    return DBUtils.getSchemaFields(resultSet);
  }

  private Map<String, Integer> getTableFields(String tableName, OracleServerConnection dbConnection) throws SQLException {
    Connection connection = dbConnection.apply();
    ResultSet resultSet = connection.createStatement().executeQuery(String.format(
      "SELECT * FROM %s WHERE 1 = 0", tableName));
    Map<String, Integer> fieldTypes = new HashMap<>();
    int columnCount = resultSet.getMetaData().getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String name = resultSet.getMetaData().getColumnLabel(i);
      int type = resultSet.getMetaData().getColumnType(i);
      fieldTypes.put(name, type);
    }
    return fieldTypes;
  }

  private List<String> getPrimaryKeys(String tableName, OracleServerConnection dbConnection) throws SQLException {
    List<String> keys = new ArrayList<>();
    Connection connection = dbConnection.apply();
    ResultSet resultSet = connection.createStatement().executeQuery(String.format(
      "SELECT column_name FROM all_cons_columns WHERE constraint_name = (" +
        "SELECT constraint_name FROM user_constraints WHERE UPPER(table_name) = UPPER('%s') " +
        "AND CONSTRAINT_TYPE = 'P')", tableName));
    while (resultSet.next()) {
      keys.add(resultSet.getString(1));
    }
    connection.close();
    return keys;
  }

  private long getCurrentSCN(OracleServerConnection dbConnection) throws SQLException {
    Connection connection = dbConnection.apply();
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT CURRENT_SCN FROM V$DATABASE");
    long changeVersion = 0;
    while (resultSet.next()) {
      changeVersion = resultSet.getLong(1);
      LOG.info("Current scn is {}", changeVersion);
    }
    connection.close();
    return changeVersion;
  }

  @Override
  public void start() {
    dbConnection = new OracleServerConnection(connection, username, password);
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


  private JdbcRDD<StructuredRecord> queryLogMinerViewContent(long prev, long cur, List<String> primaryKeys,
                                                             List<Schema.Field> fieldList) throws SQLException {

    String stmt = String.format("select operation, table_name, sql_redo from v$logmnr_contents WHERE table_space = " +
                                  "'USERS' AND table_name = '%s' AND scn > %s AND scn <= %s AND ?=?",
                                tableName, prev, cur);
    LOG.info("Querying for change data with statement {}", stmt);

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(ssc().sc(), dbConnection, stmt, 1, 1, 1,
                         new ResultSetToDMLRecord(tableName, primaryKeys, fieldList),
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
}
