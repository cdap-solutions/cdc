package co.cask.cdc.plugins;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * Created by sagarkapare on 6/12/17.
 */
public class TestKudu {
  public static void main(String[] args) throws Exception {
    String tableName = "tabletwo";
    try (KuduClient client = new KuduClient.KuduClientBuilder("demo.cask.co:7051").build()) {
      client.deleteTable(tableName);
    } catch (KuduException e) {
      e.printStackTrace();
    }
  }
}
