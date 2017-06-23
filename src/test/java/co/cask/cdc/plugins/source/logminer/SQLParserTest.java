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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SQLParserTest {

  @Test
  public void testSimpleQueryParsing() throws Exception {
    List<String> sqlQueries = new ArrayList<>();
//    sqlQueries.add("insert into \"GGTEST\".\"COMPANY\"(\"ID\",\"NAME\",\"ZIPCODE\",\"CITY\",\"DESCR\") values ('98','MyCompany','23423','Somewhere','SomeDesc')");
//    sqlQueries.add("delete from \"GGTEST\".\"COMPANY\" where \"ID\" = '20' and \"NAME\" = 'Nike' and \"ZIPCODE\" = '95131' and \"CITY\" = 'San Jose' and \"DESCR\" IS NULL and ROWID = 'AAAWuzAAGAAAAIvAAF'");
//    sqlQueries.add("update \"GGTEST\".\"COMPANY\" set \"ZIPCODE\" = '95055' where \"ID\" = '19' and \"NAME\" = 'Starbucks' and \"ZIPCODE\" = '95054' and \"CITY\" = 'Santa Clara' and \"DESCR\" IS NULL and ROWID = 'AAAWuzAAGAAAAIvAAE'");
    sqlQueries.add("alter session customers add another_customer_address varchar2(50)");

    SQLParser sqlParser = new SQLParser();
    for (String query : sqlQueries) {
      System.out.println(sqlParser.parseSQL(query));
    }
  }
}