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

package co.cask.cdc.plugins.sink.hbase;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

import java.io.IOException;

public class MultiTableOutputFormatWithQuorumAddress extends MultiTableOutputFormat {
  private final Log LOG = LogFactory.getLog(TableOutputFormat.class);
  public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";
  public static final String QUORUM_PORT = "hbase.mapred.output.quorum.port";
  public static final String REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
  public static final String REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";
  private Configuration conf = null;

  public Configuration getConf() {
    return this.conf;
  }

  public void setConf(Configuration otherConf) {
    this.conf = HBaseConfiguration.create(otherConf);
    String tableName = this.conf.get("hbase.mapred.outputtable");
    if(tableName != null && tableName.length() > 0) {
      String address = this.conf.get("hbase.mapred.output.quorum");
      int zkClientPort = this.conf.getInt("hbase.mapred.output.quorum.port", 0);
      String serverClass = this.conf.get("hbase.mapred.output.rs.class");
      String serverImpl = this.conf.get("hbase.mapred.output.rs.impl");

      try {
        if(address != null) {
          ZKUtil.applyClusterKeyToConf(this.conf, address);
        }

        if(serverClass != null) {
          this.conf.set("hbase.regionserver.impl", serverImpl);
        }

        if(zkClientPort != 0) {
          this.conf.setInt("hbase.zookeeper.property.clientPort", zkClientPort);
        }

        this.LOG.info("Created table instance for " + tableName);
      } catch (IOException var8) {
        this.LOG.error(var8);
        throw new RuntimeException(var8);
      }
    } else {
      throw new IllegalArgumentException("Must specify table name");
    }
  }
}
