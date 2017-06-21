package co.cask.cdc.plugins.source.logminer;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SQLParser implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SQLParser.class);

  public Map<String, String> parseSQL(String sqlQuery) throws Exception {
    Map<String, String> dmlFields = new HashMap<>();

    SqlParser sqlParser = SqlParser.create(sqlQuery);
    SqlNode sqlNode = sqlParser.parseQuery();

    if (sqlNode.getKind().equals(SqlKind.INSERT)) {
      SqlInsert sqlInsert = (SqlInsert) sqlNode;
      SqlNodeList sqlNodeList = sqlInsert.getTargetColumnList();

      List<String> keys = new ArrayList<>();
      for (Iterator<SqlNode> iterator = sqlNodeList.iterator(); iterator.hasNext(); ) {
        SqlNode node = iterator.next();
        keys.add(node.toString());
      }

      List<String> values = new ArrayList<>();
      List<SqlNode> valueList = ((SqlBasicCall) ((SqlBasicCall) sqlInsert.getOperandList().get(2)).getOperandList().get(0)).getOperandList();
      for (SqlNode value : valueList) {
        if (!(value instanceof SqlCharStringLiteral)) {
          LOG.info("Node {} is not an instance of SqlCharStringLiteral. It is possible that the schema of the table " +
                     "has changed and the dictionary is old. Skipping this record.");
          continue;
        }
        SqlCharStringLiteral literal = (SqlCharStringLiteral) value;
        values.add(literal.getValue().toString());
      }

      for (int i = 0; i < keys.size(); i++) {
        dmlFields.put(keys.get(i), values.get(i));
      }

      System.out.println(dmlFields);
    } else if (sqlNode.getKind().equals(SqlKind.DELETE)) {
      SqlDelete sqlDelete = (SqlDelete) sqlNode;
      SqlBasicCall delete = (SqlBasicCall) ((SqlBasicCall) sqlDelete.getOperandList().get(1)).getOperandList().get(0);

      while (true) {
        SqlNode tempNode = delete.getOperandList().get(1);
        if (!(tempNode instanceof SqlBasicCall)) {
          SqlNode[] nodes = delete.getOperands();
          if (nodes.length == 1) {
            dmlFields.put(nodes[0].toString(), null);
          } else {
            dmlFields.put(nodes[0].toString(), nodes[1].toString());
          }
          break;
        }
        SqlBasicCall basicCall = (SqlBasicCall) delete.getOperandList().get(1);
        SqlNode[] nodes = basicCall.getOperands();
        if (nodes.length == 1) {
          dmlFields.put(nodes[0].toString(), null);
        } else {
          dmlFields.put(nodes[0].toString(), nodes[1].toString());
        }
        delete = ((SqlBasicCall) delete.getOperandList().get(0));
      }
      System.out.println(dmlFields);
    } else if (sqlNode.getKind().equals(SqlKind.UPDATE)) {
      SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
      List<SqlNode> sqlNodeList = sqlUpdate.getOperandList();
      int i;
      for (i = 1; i < sqlNodeList.size(); i += 2) {
        if (sqlNodeList.get(i) instanceof SqlBasicCall) {
          break;
        }
        String key = ((SqlNodeList) sqlNodeList.get(i)).get(0).toString();
        String value = ((NlsString) ((SqlCharStringLiteral) ((SqlNodeList) sqlNodeList.get(i + 1)).get(0)).getValue()).getValue();
        dmlFields.put(key, value);
      }
      SqlBasicCall update = ((SqlBasicCall) ((SqlBasicCall) sqlNodeList.get(i)).getOperandList().get(0));
      while (true) {
        SqlNode tempNode = update.getOperandList().get(1);
        if (!(tempNode instanceof SqlBasicCall)) {
          SqlNode[] nodes = update.getOperands();
          if (!dmlFields.keySet().contains(nodes[0].toString())) {
            if (nodes.length == 1) {
              dmlFields.put(nodes[0].toString(), null);
            } else {
              dmlFields.put(nodes[0].toString(), nodes[1].toString());
            }
          }
          break;
        }

        SqlBasicCall basicCall = (SqlBasicCall) update.getOperandList().get(1);
        SqlNode[] nodes = basicCall.getOperands();

        if (!dmlFields.keySet().contains(nodes[0].toString())) {
          if (nodes.length == 1) {
            dmlFields.put(nodes[0].toString(), null);
          } else {
            dmlFields.put(nodes[0].toString(), nodes[1].toString());
          }
        }
        update = ((SqlBasicCall) update.getOperandList().get(0));
      }
    }
    return dmlFields;
  }
}
