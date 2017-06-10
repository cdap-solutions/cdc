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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SQLQueryToStructuredRecordTest {

  @Test
  public void testSimpleQueryParsing() throws Exception {
    List<String> sqlQueries = new ArrayList<>();
    sqlQueries.add("insert into \"GGTEST\".\"COMPANY\"(\"ID\",\"NAME\",\"ZIPCODE\",\"CITY\",\"DESCR\") values ('98','MyCompany','23423','Somewhere','SomeDesc')");
    sqlQueries.add("delete from \"GGTEST\".\"COMPANY\" where \"ID\" = '20' and \"NAME\" = 'Nike' and \"ZIPCODE\" = '95131' and \"CITY\" = 'San Jose' and \"DESCR\" IS NULL and ROWID = 'AAAWuzAAGAAAAIvAAF'");
    sqlQueries.add("update \"GGTEST\".\"COMPANY\" set \"ZIPCODE\" = '95055' where \"ID\" = '19' and \"NAME\" = 'Starbucks' and \"ZIPCODE\" = '95054' and \"CITY\" = 'Santa Clara' and \"DESCR\" IS NULL and ROWID = 'AAAWuzAAGAAAAIvAAE'");

    for (String sqlQuery : sqlQueries) {
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
          SqlCharStringLiteral literal = (SqlCharStringLiteral) value;
          values.add(literal.getValue().toString());
        }

        Assert.assertEquals(keys.size(), values.size());

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
        System.out.println(dmlFields);
      }
    }
  }
}
