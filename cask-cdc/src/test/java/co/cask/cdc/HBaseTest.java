package co.cask.cdc;

import co.cask.cdap.api.common.Bytes;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sagarkapare on 5/30/17.
 */
public class HBaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTest.class);

  public static void main(String[] args) throws Exception {
    // Job job = JobUtils.createInstance();
    // Configuration conf = job.getConfiguration();
    Configuration conf = HBaseConfiguration.create();
    String zkQuorum = "cdc-demo22755-1000.dev.continuuity.net";
    String zkClientPort = "2181";
    String zkNodeParent = "/hbase";
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set("hbase.zookeeper.property.clientPort", zkClientPort);
    conf.set("zookeeper.znode.parent", zkNodeParent);
    LOG.info("Zookeeper quorum to HBASEXXX {}", String.format("%s:%s:%s", zkQuorum, zkClientPort, zkNodeParent));

    try (Connection connection = ConnectionFactory.createConnection(conf);
         Admin hBaseAdmin = connection.getAdmin()) {
      if (!hBaseAdmin.tableExists(TableName.valueOf("sometable"))) {
        LOG.info("Table does not exists!!!");
      } else {
        LOG.info("Table does exists!!!");
      }
    }
  }
}
