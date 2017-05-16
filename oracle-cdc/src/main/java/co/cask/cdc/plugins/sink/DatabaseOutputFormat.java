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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();


  public class DatabaseRecordWriter extends RecordWriter<DatabaseRecord,NullWritable> {

    private Connection connection;
    private Map<String, PreparedStatement> map;
    private String tableName;

    public DatabaseRecordWriter() throws SQLException {
    }

    public DatabaseRecordWriter(Connection connection, String tableName) throws SQLException {
      this.connection = connection;
      this.map = new HashMap<>();
      this.tableName = tableName;
    }

    public Connection getConnection() {
      return connection;
    }

    public PreparedStatement getStatement(String type) {
      return map.get(type);
    }

    /** {@inheritDoc} */
    public void close(TaskAttemptContext context) throws IOException {
      try {
        for (PreparedStatement statement : map.values()) {
          statement.executeBatch();
          connection.commit();
        }
      } catch (SQLException e) {
        try {
          connection.rollback();
        }
        catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e);
      } finally {
        try {
          for (PreparedStatement statement : map.values()) {
            statement.close();
            connection.close();
          }
        }
        catch (SQLException ex) {
          throw new IOException(ex);
        }
      }
    }

    @Override
    public void write(DatabaseRecord record, NullWritable nullWritable) throws IOException {
      try {
        StructuredRecord input = record.getRecord();
        if (input.getSchema().getRecordName().equals("DDLRecord")) {
          Statement selectStatement = connection.createStatement();
          ResultSet rs = selectStatement.executeQuery(String.format("SELECT TOP 1 * FROM %s", tableName));
          ResultSetMetaData resultSetMetadata = rs.getMetaData();

          Set<String> oldColumns = new HashSet<>();
          // JDBC driver column indices start with 1
          for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            String name = resultSetMetadata.getColumnName(i + 1);
            oldColumns.add(name);
          }
          selectStatement.close();

          org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse((String) input.get("schema"));
          Schema newSchema = AvroConverter.fromAvroSchema(avroSchema);
          Schema.Field beforeField = newSchema.getField("before");
          Set<String> newColumns = new HashSet<>();
          Map<String,  Schema> newColumnsMap = new HashMap<>();
          for (Schema.Field field : beforeField.getSchema().getNonNullable().getFields()) {
            if (!field.getName().endsWith("_isMissing")) {
              // This is a column in the db, add it to set
              newColumns.add(field.getName());
              newColumnsMap.put(field.getName(), field.getSchema());
            }
          }

          Sets.SetView<String> columnDiff = Sets.symmetricDifference(newColumns, oldColumns);
          LOG.info("Column Diff {}", columnDiff);
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

          LOG.info("Columns to add {} and Columns to delete {}", columnsToAdd, columnsToDelete);

          if (!columnDiff.isEmpty()) {
            alterTable(connection, tableName, newColumnsMap, columnsToDelete);
          }

          // Update cached prepared statements
          map.clear();
          return;
        }

        String operationType = input.get("op_type");
        PreparedStatement preparedStatement;
        LOG.info("#### Operation type {}", operationType);
        switch (operationType) {
          case "I":
            if (!map.containsKey("I")) {
              StructuredRecord insertRecord = input.get("after");
              // create the insert query, construct prepared statement and cache the prepared statement
              String insertQuery = constructInsertQuery(tableName, insertRecord.getSchema().getFields());
              preparedStatement = connection.prepareStatement(insertQuery);
              map.put("I", preparedStatement);
            } else {
              preparedStatement = map.get("I");
              record.write(preparedStatement);
              preparedStatement.executeUpdate();
            }
            break;
          case "U":
            if (!map.containsKey("U")) {
              // create the update query, construct prepared statement and cache the prepared statement
              StructuredRecord updateRecord = input.get("after");
              List<String> primaryKeys = input.get("primary_keys");
              String updateQuery = constructUpdateQuery(tableName, updateRecord.getSchema().getFields(), primaryKeys);
              preparedStatement = connection.prepareStatement(updateQuery);
              map.put("U", preparedStatement);
            } else {
              preparedStatement = map.get("U");
              record.write(preparedStatement);
              preparedStatement.executeUpdate();
            }
            break;
          case "D":
            if (!map.containsKey("D")) {
              StructuredRecord deleteRecord = input.get("before");
              // create the delete query, construct prepared statement and cache the prepared statement
              String deleteQuery = constructDeleteQuery(tableName, deleteRecord.getSchema().getFields());
              preparedStatement = connection.prepareStatement(deleteQuery);
              map.put("D", preparedStatement);
            } else {
              preparedStatement = map.get("D");
              record.write(preparedStatement);
              preparedStatement.executeUpdate();
            }
            break;
          case "T":
            if (!map.containsKey("T")) {
              // create the truncate query, construct prepared statement and cache the prepared statement
              // Example: TRUNCATE TABLE tableName
              String deleteQuery = "TRUNCATE TABLE " + tableName;
              preparedStatement = connection.prepareStatement(deleteQuery);
              map.put("T", preparedStatement);
            } else {
              preparedStatement = map.get("T");
              // do not need to add any field values in truncate statement, just add it to batch for execution
              preparedStatement.executeUpdate();
            }
            break;
          default:
            throw new RuntimeException("Illegal operation type " + operationType);

        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException(e);
      }
    }
  }

  private void alterTable(Connection connection, String tableName, Map<String, Schema> columnsToAdd,
                          Set<String> columnsToDelete) throws SQLException {
    // drop columns
    if (!columnsToDelete.isEmpty()) {
      String dropColumns = "ALTER TABLE " + tableName + " DROP COLUMN " + Joiner.on(", ").join(columnsToDelete);
      LOG.info("### Alter drop query is: {}", dropColumns);
      executeStatement(connection, dropColumns);
    }

    // add columns
    String alterQuery = "ALTER TABLE " + tableName + " ADD ";
    for (Map.Entry<String, Schema> columnToAdd : columnsToAdd.entrySet()) {
      try {
         alterQuery = alterQuery + columnToAdd.getKey() + " " +
           toDatabaseType(columnToAdd.getKey(), columnToAdd.getValue());
      } catch (TypeConversionException e) {
        throw new SQLException(e);
      }
    }

    if (!columnsToAdd.isEmpty()) {
      LOG.info("### Alter add query is: {}", alterQuery);
      executeStatement(connection, alterQuery);
    }
  }

  private void executeStatement(Connection connection, String dropColumns) throws SQLException {
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = connection.prepareStatement(dropColumns);
      preparedStatement.executeUpdate();
      connection.commit();
    } finally {
      if (preparedStatement != null) {
          preparedStatement.close();
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
      return "DOUBLE";
    } else if (type == Schema.Type.FLOAT) {
      return "FLOAT";
    } else if (type == Schema.Type.BOOLEAN) {
      return "BOOLEAN";
    } else if (type == Schema.Type.UNION) { // Recursively drill down into the non-nullable type.
      return toDatabaseType(name, schema.getNonNullable());
    } else {
      throw new TypeConversionException(
        String.format("Field '%s' is having a type '%s' that is not supported by Database Sink. Please change the " +
                        "type.", name, type.toString())
      );
    }
  }

  public String constructInsertQuery(String tableName, List<Schema.Field> fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    // Example: INSERT INTO tableName (column1, column2, column3, column4) VALUES (?,?,?,?)
    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(tableName);

    if (fieldNames.size() > 0 && fieldNames.get(0) != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.size(); i++) {
        if (!fieldNames.get(i).getName().endsWith("_isMissing")) {
          if (i > 0 && i < fieldNames.size() - 1) {
            query.append(", ");
          }
          query.append(fieldNames.get(i).getName());
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.size(); i++) {
      if (!fieldNames.get(i).getName().endsWith("_isMissing")) {
        if (i > 0 && i < fieldNames.size() - 1) {
          query.append(", ");
        }
        query.append("?");
      }
    }
    query.append(")");

    LOG.info("### INSERT query created is: {}", query.toString());
    return query.toString();
  }

  private String constructUpdateQuery(String tableName, List<Schema.Field> fieldNames, List<String> primaryKeys) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    // Example: UPDATE table_name SET column1 = value1, column2 = value2 WHERE primarykey1 = 1 AND primarykey2 = 2
    StringBuilder query = new StringBuilder();
    query.append("UPDATE ").append(tableName).append(" SET ");

    // generate SET Values
    if (fieldNames.size() > 0 && fieldNames.get(0) != null) {
      for (int i = 0; i < fieldNames.size(); i++) {
        if (!fieldNames.get(i).getName().endsWith("_isMissing")) {
          if (i > 0 && i < fieldNames.size() - 1) {
            query.append(", ");
          }
          query.append(fieldNames.get(i).getName()).append(" = ?");
        }
      }
    }

    query.append(" WHERE ");

    // generate WHERE Clause
    for (int i = 0; i < primaryKeys.size(); i++) {
      if (i > 0 && i < fieldNames.size() - 1) {
        query.append(" AND ");
      }
      query.append(primaryKeys.get(i)).append(" = ?");
    }

    LOG.info("### UPDATE query created is: {}", query.toString());
    return query.toString();
  }

  private String constructDeleteQuery(String tableName, List<Schema.Field> fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    // Example: DELETE FROM tableName WHERE col1 = ? AND col2 = ? AND col3 = ?
    StringBuilder query = new StringBuilder();
    query.append("DELETE FROM ").append(tableName).append(" WHERE ");

    if (fieldNames.size() > 0 && fieldNames.get(0) != null) {
      for (int i = 0; i < fieldNames.size(); i++) {
        if (!fieldNames.get(i).getName().endsWith("_isMissing")) {
          if (i > 0 && i < fieldNames.size() - 1) {
            query.append(" AND ");
          }
          query.append(fieldNames.get(i).getName()).append(" = ?");
        }
      }
    }
    LOG.info("### Delete query created is: {}", query.toString());
    return query.toString();
  }

  /** {@inheritDoc} */
  public RecordWriter<DatabaseRecord,NullWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException {
    DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
    String tableName = dbConf.getOutputTableName();

    try {
      Connection connection = getConnection(context.getConfiguration());
      return new DatabaseRecordWriter(connection, tableName);
    } catch (Exception ex) {
      LOG.error("#### Error occurred for table: {}", tableName, ex);
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

      connection.setAutoCommit(true);
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
