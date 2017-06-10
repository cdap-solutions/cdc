package co.cask.cdc.plugins.source.logminer;

import oracle.jdbc.driver.OracleDriver;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rsinha on 6/9/17.
 */
public class Test {

  public static void main(String[] argv) {

    System.out.println("-------- Oracle JDBC Connection Testing ------");

    try {

      Class.forName(OracleDriver.class.getName());

    } catch (ClassNotFoundException e) {

      System.out.println("Where is your Oracle JDBC Driver?");
      e.printStackTrace();
      return;

    }

    System.out.println("Oracle JDBC Driver Registered!");

    Connection connection = null;

    try {

      connection = DriverManager.getConnection(
        "jdbc:oracle:thin:@107.178.216.167:1521:xe", "ggtest", "ggtest");

    } catch (SQLException e) {

      System.out.println("Connection Failed! Check output console");
      e.printStackTrace();
      return;

    }

    if (connection != null) {
      System.out.println("You made it, take control your database now!");
    } else {
      System.out.println("Failed to make connection!");
    }

    List<String> redoFiles = new ArrayList<>();
    PreparedStatement statement = null;
    try {
      ResultSet resultSet = connection.createStatement().executeQuery("SELECT distinct member LOGFILENAME FROM V$LOGFILE");

        while (resultSet.next()) {
          redoFiles.add(resultSet.getString(1));
        }

      for (String redoFile : redoFiles) {
        String addFileQuery = String.format("BEGIN DBMS_LOGMNR.ADD_LOGFILE('%s'); END;", redoFile);
        System.out.println(addFileQuery);
        CallableStatement callableStatement = connection.prepareCall(addFileQuery);
        System.out.println(callableStatement.execute());
      }

      // Start LogMiner
      // execute DBMS_LOGMNR.START_LOGMNR (options => dbms_logmnr.dict_from_online_catalog);
      String one = "BEGIN DBMS_LOGMNR.START_LOGMNR (options => dbms_logmnr.dict_from_online_catalog " +
        "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.NO_SQL_DELIMITER); END;";
//      CallableStatement callableStatement = connection.prepareCall(
//        "BEGIN DBMS_LOGMNR.START_LOGMNR (options => dbms_logmnr.dict_from_online_catalog " +
//          "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.NO_SQL_DELIMITER); END;");
//      System.out.println(callableStatement.execute());

      String stmt = "select operation, table_name, sql_redo from v$logmnr_contents WHERE table_space = 'USERS' AND scn " +
        ">= 4969561 AND 1=1";

      resultSet = connection.createStatement().executeQuery(one+ "\n" + stmt);


      while (resultSet.next()) {
        System.out.println(resultSet.getString("OPERATION"));
      }

      connection.close();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.out.println(redoFiles);



  }


}
