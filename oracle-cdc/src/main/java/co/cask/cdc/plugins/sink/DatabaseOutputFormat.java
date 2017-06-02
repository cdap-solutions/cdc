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


package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Database output format
 */
public class DatabaseOutputFormat extends DBOutputFormat<DatabaseRecord,NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseOutputFormat.class);
  private Object driver;

  public class DatabaseRecordWriter extends RecordWriter<DatabaseRecord,NullWritable> {

    private Connection connection;

    public DatabaseRecordWriter() throws SQLException {
    }

    public DatabaseRecordWriter(Connection connection) throws SQLException {
      this.connection = connection;
    }

    /** {@inheritDoc} */
    public void close(TaskAttemptContext context) throws IOException {
      try {
        commitChanges();
        connection.close();
      } catch (SQLException e) {
        LOG.warn(StringUtils.stringifyException(e));
        throw new IOException(e);
      }
    }

    private void commitChanges() throws IOException {
      try {
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        }
        catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e);
      }
    }

    @Override
    public void write(DatabaseRecord record, NullWritable nullWritable) throws IOException {
      PreparedStatement preparedStatement = null;
      try {
        StructuredRecord input = record.getRecord();
        String namespacedTableName = input.get("table");
        String tableName = namespacedTableName.split("\\.")[1];

        if (input.getSchema().getRecordName().equals("DDLRecord")) {
          if (!tableExists(connection, tableName)) {
            // Do not do alter on non-existing table. If table does not exist,
            // defer table creation when first insert happens
            return;
          }
          alterTableSchema(input, tableName);
          return;
        }

        String operationType = input.get("op_type");
        List<String> primaryKeys = input.get("primary_keys");
        StructuredRecord change = input.get("change");

        List<Schema.Field> fields = change.getSchema().getFields();

        if (!tableExists(connection, tableName)) {
          // Create table if it does not exist
          createTable(tableName, primaryKeys, change, fields);
          return;
        }

        switch (operationType) {
          case "I":
            preparedStatement = connection.prepareStatement(constructInsertQuery(tableName, fields, change));
            record.write(preparedStatement);
            break;
          case "U":
            preparedStatement = connection.prepareStatement(constructUpdateQuery(tableName,
                                                                                 change, fields, primaryKeys));
            record.write(preparedStatement);
            break;
          case "D":
            preparedStatement = connection.prepareStatement(constructDeleteQuery(tableName, primaryKeys));
            record.write(preparedStatement);
            break;
          case "T":
            // create the truncate query, construct prepared statement and cache the prepared statement
            // Example: TRUNCATE TABLE tableName
            preparedStatement = connection.prepareStatement("TRUNCATE TABLE " + tableName);
            break;
          default:
            throw new RuntimeException("Illegal operation type " + operationType);
        }
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        if (preparedStatement != null) {
          try {
            preparedStatement.executeUpdate();
            preparedStatement.close();
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }
      }
    }

    private void createTable(String tableName, List<String> primaryKeys, StructuredRecord change,
                             List<Schema.Field> fields) throws TypeConversionException, SQLException, IOException {
        /*
        CREATE TABLE employee(
        EMPNO     BIGINT,
        ENAME     VARCHAR(255),
        JOB    VARCHAR(255),
        MGR   BIGINT,
        HIREDATE VARCHAR(255),
        SAL       BIGINT,
        COMM       BIGINT,
        DEPTNO       BIGINT,
        EMP_ADDRESS  VARCHAR(255),
        PRIMARY KEY (ENAME, EMPNO));
         */

      StringBuilder query = new StringBuilder();
      query.append("CREATE TABLE ").append(tableName);

      if (fields.size() > 0 && fields.get(0) != null) {
        query.append(" (");
        for (int i = 0; i < fields.size(); i++) {
          if (i > 0 && i < fields.size()) {
            query.append(", ");
          }
          query.append(fields.get(i).getName()).append(" ").append(toDatabaseType(fields.get(i).getName(),
                                                                                  fields.get(i).getSchema()));
        }

        if (primaryKeys.size() > 0) {
          query.append(", PRIMARY KEY (");

          for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0 && i < fields.size()) {
              query.append(", ");
            }
            query.append(primaryKeys.get(i));
          }
          query.append(")");
        }
        query.append(")");
      }

      LOG.debug("Create statement is: {}", query.toString());

      PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
      preparedStatement.executeUpdate();
      commitChanges();
      preparedStatement.close();
    }


    private boolean tableExists(Connection connection, String tableName) throws SQLException {
      DatabaseMetaData md = connection.getMetaData();
      ResultSet rs = md.getTables(null, null, tableName, null);
      if (rs.next()) {
        return true;
      }
      return false;
    }

    private void alterTableSchema(StructuredRecord input, String tableName) throws SQLException, IOException {
      Statement selectStatement = connection.createStatement();
      // get first record from the table to get existing table schema
      ResultSet rs = selectStatement.executeQuery(String.format("SELECT TOP 1 * FROM %s", tableName));
      ResultSetMetaData resultSetMetadata = rs.getMetaData();

      Set<String> oldColumns = new HashSet<>();
      // JDBC driver column indices start with 1
      for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
        String name = resultSetMetadata.getColumnName(i + 1);
        oldColumns.add(name);
      }
      selectStatement.close();

      LOG.debug("OldColumns {}", oldColumns);

      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse((String) input.get("schema"));
      Schema newSchema = AvroConverter.fromAvroSchema(avroSchema);
      Set<String> newColumns = new HashSet<>();
      Map<String,  Schema> newColumnsMap = new HashMap<>();

      for (Schema.Field field : newSchema.getFields()) {
        newColumns.add(field.getName());
        newColumnsMap.put(field.getName(), field.getSchema());
      }

      LOG.debug("NewColumns {}", newColumns);

      Sets.SetView<String> columnDiff = Sets.symmetricDifference(newColumns, oldColumns);
      Set<String> columnsToDelete = new HashSet<>();
      Set<String> columnsToAdd = new HashSet<>();
      for (String column : columnDiff) {
        if (oldColumns.contains(column)) {
          // This column is removed
          columnsToDelete.add(column);
        } else {
          // This column is added
          columnsToAdd.add(column);
        }
      }

      if (!columnsToAdd.isEmpty()) {
        newColumnsMap.keySet().retainAll(columnsToAdd);
      } else {
        newColumnsMap.clear();
      }

      LOG.debug("Columns to add {} and Columns to delete {}", columnsToAdd, columnsToDelete);

      if (!columnDiff.isEmpty()) {
        commitChanges();
        alterTable(tableName, newColumnsMap, columnsToDelete);
      }
    }

    private void alterTable(String tableName, Map<String, Schema> columnsToAdd,
                            Set<String> columnsToDelete) throws SQLException {
      // add columns
      String alterQuery = "ALTER TABLE " + tableName + " ADD ";
      String prefix = "";
      for (Map.Entry<String, Schema> columnToAdd : columnsToAdd.entrySet()) {
        try {
          alterQuery = alterQuery + prefix + columnToAdd.getKey() + " " +
            toDatabaseType(columnToAdd.getKey(), columnToAdd.getValue());
          prefix = ", ";
        } catch (TypeConversionException e) {
          throw new SQLException(e);
        }
      }

      if (!columnsToAdd.isEmpty()) {
        LOG.debug("Alter query to add columns {}", alterQuery);
        executeDDL(alterQuery);
      }

      // drop columns
      if (!columnsToDelete.isEmpty()) {
        String dropQuery = "ALTER TABLE " + tableName + " DROP COLUMN " + Joiner.on(", ").join(columnsToDelete);
        LOG.debug("Alter query to drop columns {}", dropQuery);
        executeDDL(dropQuery);
      }
      // commit both the updates to the table
      connection.commit();
    }

    private void executeDDL(String query) throws SQLException {
      PreparedStatement preparedStatement = null;
      try {
        preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
      } finally {
        if (preparedStatement != null) {
          preparedStatement.close();
        }
      }
    }
  }

  private String toDatabaseType(String name, Schema schema) throws TypeConversionException {
    Schema.Type type = schema.getType();
    if (type == Schema.Type.STRING) {
      return "VARCHAR(255)";
    } else if (type == Schema.Type.INT) {
      return "INTEGER";
    } else if (type == Schema.Type.LONG) {
      return "BIGINT";
    } else if (type == Schema.Type.BYTES) {
      return "BINARY";
    } else if (type == Schema.Type.DOUBLE) {
      return "FLOAT";
    } else if (type == Schema.Type.FLOAT) {
      return "REAL";
    } else if (type == Schema.Type.BOOLEAN) {
      return "BIT";
    } else if (type == Schema.Type.UNION) { // Recursively drill down into the non-nullable type.
      return toDatabaseType(name, schema.getNonNullable());
    } else {
      throw new TypeConversionException(
        String.format("Field '%s' is having a type '%s' that is not supported by Database Sink. Please change the " +
                        "type.", name, type.toString())
      );
    }
  }

  public String constructInsertQuery(String tableName, List<Schema.Field> fieldNames, StructuredRecord change) {
    if (fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    // Example: INSERT INTO tableName (column1, column2, column3, column4) VALUES (?,?,?,?)
    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(tableName);

    if (fieldNames.size() > 0 && fieldNames.get(0) != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0 && i < fieldNames.size()) {
          query.append(", ");
        }
        query.append(fieldNames.get(i).getName());
      }

      query.append(")");
    }

    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0 && i < fieldNames.size()) {
        query.append(", ");
      }
      query.append("?");
    }

    query.append(")");

    LOG.debug("Insert query is {}", query.toString());

    return query.toString();
  }

  private String constructUpdateQuery(String tableName, StructuredRecord change, List<Schema.Field> fieldNames,
                                      List<String> primaryKeys) {

    // Example: UPDATE table_name SET column1 = value1, column2 = value2 WHERE primarykey1 = 1 AND primarykey2 = 2
    StringBuilder query = new StringBuilder();
    query.append("UPDATE ").append(tableName).append(" SET ");

    if (fieldNames.size() > 0 && fieldNames.get(0) != null) {
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0 && i < fieldNames.size()) {
          query.append(", ");
        }
        query.append(fieldNames.get(i).getName()).append(" = ?");
      }
    }

    query.append(" WHERE ");

    // generate WHERE Clause
    for (int i = 0; i < primaryKeys.size(); i++) {
      if (i > 0 && i < fieldNames.size()) {
        query.append(" AND ");
      }
      query.append(primaryKeys.get(i)).append(" = ?");
    }

    LOG.debug("Update query is {}", query.toString());
    return query.toString();
  }

  private String constructDeleteQuery(String tableName, List<String> primaryKeys) {

    // Example: DELETE FROM tableName WHERE col1 = ? AND col2 = ? AND col3 = ?
    StringBuilder query = new StringBuilder();
    query.append("DELETE FROM ").append(tableName).append(" WHERE ");

    for (int i = 0; i < primaryKeys.size(); i++) {
      if (i > 0 && i < primaryKeys.size()) {
        query.append(" AND ");
      }
      query.append(primaryKeys.get(i)).append(" = ?");
    }

    LOG.debug("Delete query is: {}", query.toString());
    return query.toString();
  }

  public RecordWriter<DatabaseRecord,NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
    try {
      Connection connection = getConnection(context.getConfiguration());
      return new DatabaseRecordWriter(connection);
    } catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  private Connection getConnection(Configuration conf) {
    Connection connection;
    try {
      String url = conf.get(DBConfiguration.URL_PROPERTY);
      try {
        // throws SQLException if no suitable driver is found
        DriverManager.getDriver(url);
      } catch (SQLException e) {


          if (driver == null) {
            ClassLoader classLoader = conf.getClassLoader();
            @SuppressWarnings("unchecked")
            Class<? extends Driver> driverClass =
              (Class<? extends Driver>) classLoader.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
            driver = driverClass.newInstance();
          }

        driver = Class.forName(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY)).newInstance();
        DriverManager.registerDriver((Driver) driver);
        LOG.debug("Registered JDBC driver via shim {}. Actual Driver {}.", driver);
      }

      if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
        connection = DriverManager.getConnection(url);
      } else {
        connection = DriverManager.getConnection(url,
                                                 conf.get(DBConfiguration.USERNAME_PROPERTY),
                                                 conf.get(DBConfiguration.PASSWORD_PROPERTY));
      }

      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return connection;
  }

  /**
   * Initializes the reduce-part of the job with
   * the appropriate output settings
   *
   * @param job The job
   * @param tableName The table to run commands on
   */
  public static void setOutput(Job job, String tableName) throws IOException {
    job.setOutputFormatClass(DBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());

    dbConf.setOutputTableName(tableName);
  }
}
