package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Throwables;
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
      this.connection.setAutoCommit(false);
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
        LOG.info("#### Got message {}", GSON.toJson(input));

        if (input.getSchema().getRecordName().equals("DDLRecord")) {
          // TODO support DDL operations
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
              preparedStatement.addBatch();
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
              preparedStatement.addBatch();
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
              preparedStatement.addBatch();
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
              preparedStatement.addBatch();
            }
            break;
          default:
            throw new RuntimeException("Illegal operation type " + operationType);

        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
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
