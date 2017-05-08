package co.cask.cdc;

import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        String tableName = "test3";
        try (KuduClient client = new KuduClient.KuduClientBuilder("demo.cask.co:7051").build()) {

            KuduTable kuduTable = client.openTable(tableName);
            System.out.println(kuduTable.getSchema().getColumns());
            client.deleteTable(tableName);
            /*
            client.alterTable(tableName, new AlterTableOptions().addColumn("STATE", Type.STRING, "NA"));
            boolean isDone = client.isAlterTableDone(tableName);
            if (isDone) {
                System.out.println("Added Column!");
            } else {
                System.out.println("Add Column Failed");
            }
            kuduTable = client.openTable(tableName);
            System.out.println(kuduTable.getSchema().getColumns());
            */
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
