/*
 *
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

package co.cask.hydrator.sqlcdc

import co.cask.cdap.api.dataset.table.Row
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * An InputDStream that periodically refreshes its contents. Each interval, it simply returns the table state
 * since the last refresh.
 *
 * @param sec the CDAP spark execution context
 * @param ssc the Spark streaming context
 * @param name the table name
 */
class TableInputDStream(ssc: StreamingContext,
                        connection: String,
                        username: String,
                        password: String,
                        @transient var sparkContext: SparkContext)
  extends InputDStream[(Array[Byte], Row)](ssc: StreamingContext) {

  override def start(): Unit = {
    // no-op
  }

  override def stop(): Unit = {
  }

  override def compute(validTime: Time): Option[RDD[(Array[Byte], Row)]] = {

    Some(table)
  }

  def getChangeData(): Unit = {
    val dbConnection = new SQLInputDstream#SQLServerConnection(connection, username, password)
    val tableName = "tone";
    val stmt = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + tableName + "(sys.fn_cdc_get_min_lsn('" + tableName +
      "'), sys.fn_cdc_get_max_lsn(), 'all') WHERE ? = ?"
    //TODO Currently we are not partitioning the data. We should partition it for scalability

    new JdbcRDD[Array[AnyRef]](sparkContext, dbConnection, stmt, 1, 1, 1)
  }
}
