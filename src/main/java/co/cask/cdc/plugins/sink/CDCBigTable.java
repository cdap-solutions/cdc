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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * BigTable sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCBigTable")
public class CDCBigTable extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCBigTable.class);

  private final CDCBigTableConfig config;
//  private final String CDC_COLUMN_FAMILY = "cdc";

  public CDCBigTable(CDCBigTableConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception { }

  public static final String PROJECT_ID = "cask-gae-ip-geolocation";
  public static final String INSTANCE_ID = "testinstance";
  public static final String CREDENTIALS = "{\n" +
    "  \"type\": \"service_account\",\n" +
    "  \"project_id\": \"cask-gae-ip-geolocation\",\n" +
    "  \"private_key_id\": \"107abf9804c5f562d5c20584796675eace7aaa31\",\n" +
    "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2rO9SUgVn5/6U\\naMiJs21jm38mfywjDhXYgfwV8i7WTSk8oKafsOMHvVqexVVdzhZw4eirCfBwW4Bl\\nhMtUpEhBqXS0yrwUQQnZnjjsFlib82SXtkEtGP9qXIxg8aWBnqmwH7940gbwRi0/\\n+DD6I0aTXskUi/2kDCyNtVwIteUw6z8iy0x2J75cmRNX9vXnxEq6qL2x72ML3JOe\\nejudZHFZjvJuRWqVSntsHS8H2d8wLVZfRBrT8WgiMZzFw/2AIE4lh/QHNGQWnrFo\\nBzEB+u98c91Bs/vdbLinr3yCitD2eOIisRkupydk53d2qN/aGB7wi95CMawbi/x/\\neFaYcjI1AgMBAAECggEAKrCcVxUS7VbigCVCpCd5y1rV6pWyp1iN02SBGFHaDzvG\\nBMSYW95XGAJw0ITWL89PID8zA/GVLnS7uz2+1L91oV6sBuoP7P3MAv6+V7HbMEq/\\nedLeRV8/pUgOiENAhduh1SZ+NQqEE5ea0IW8UGB5CEyQ2kvi0PkFWnKFGQYFExqp\\nfo0eZqy5dRpi5aPZh56N60CEmJgCwWauLjC1mWapk/Sdmi11jVk0mWRS/AXODSyo\\nf7HHXHnk1Cn6agsTknMc0y0zO72Phe4iqMb7wQUXY4VdNRb1pOUJoHAT6OlLRSf8\\nrkHsiim0sqItJh0V1AUBTmqN4XGFvL/PL2o3qk4CiwKBgQDnaSPHBUg66h84vwiP\\n1zNXQOjS8PGecbSpXA5wVHhEMf/3KYqcfmr7rXEtVNBuTjl3ST5PQfsL4YOH2sgu\\n027vCRVfJKziWh0OafXvm9EyMilns8CIjfzGPV2hUYjZhh+ITrlspCKyqHtoNup8\\ny/rLTUkb5pJItXeKtoWcBOuygwKBgQDKFhhmKX08IHMEdbvGMZgFWwloTrM2e8tF\\nMzh1Cp78h0HurVcP7u2S8MKMi7wfwZr8bGjyvNt8Wl/UJYUg1C3eisq2ojaNGk8Q\\nTWDNVFSj5emZivkQE0KKP0zPkPcy8gddt/FgM6lj7F8XoHQMKxfMA0ZMP8S+VSnV\\nIJqOLBkK5wKBgQCVjiK8NhK3WKXy86th4u/gXSfbZDKTduMObVs7h6vuTu4hW6yk\\ndSNJIo+5f03xbAbBrAlkCb1osUdjXqbvdGAGhjVUtwwwgZKzxRFX2Lj7muWditNM\\nrY8Gw5QmdN5fzsnEOzSlHPL7yd1vvcYP/3hHOdyc9ofmC7mFYW66JYf6ZwKBgH4u\\nh7OaqljVxht9y+533v1RF7GqmloluAmQbusd46G4buGyGE+Zl3wNmtyZD7EgeT6u\\nDmWqqL2fzIAxoUubULzJGsQoyzkVuVJrjksHIgZos5Cs5tEzxXN/DN36HXAREapi\\nBXRVLap31/RvuqYybhxz2vwXQSi7EnDCd97YCBb3AoGAUCdYPVrhAShXlzAG2S/W\\n7MQT7OjNb5pjok/xB5cwteGXmNCgW8F1K2BwVpunf7jQGd1wTpSNvmr5+LnIR0M9\\n4EgGqsnPL2ywjZLBMFqLA4fV8SQXWzO1asVRF2udCZMpTKXnjjMRiKIP6VmpEEfK\\nrWy42uHZnW7Y8sGNQsrL/aw=\\n-----END PRIVATE KEY-----\\n\",\n" +
    "  \"client_email\": \"bigtableserviceacc@cask-gae-ip-geolocation.iam.gserviceaccount.com\",\n" +
    "  \"client_id\": \"106472780083920424348\",\n" +
    "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
    "  \"token_uri\": \"https://accounts.google.com/o/oauth2/token\",\n" +
    "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
    "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bigtableserviceacc%40cask-gae-ip-geolocation.iam.gserviceaccount.com\"\n" +
    "}\n";


  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    // Get the hadoop configurations and passed it as a Map to the closure
    Iterator<Map.Entry<String, String>> iterator = javaRDD.context().hadoopConfiguration().iterator();
    final Map<String, String> configs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      configs.put(next.getKey(), next.getValue());
    }

    // maps data sets to each block of computing resources
    javaRDD.foreachPartition(new VoidFunction<Iterator<StructuredRecord>>() {

      @Override
      public void call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {

        Job job;
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
        // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
        // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
        // points to).
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
          job = JobUtils.createInstance();
          Configuration conf = job.getConfiguration();

          for (Map.Entry<String, String> configEntry : configs.entrySet()) {
            conf.set(configEntry.getKey(), configEntry.getValue());
          }

          BigtableConfiguration.configure(conf, config.project, config.instance);
          conf.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY, CDCBigTable.CREDENTIALS);

//        try (Connection connection = ConnectionFactory.createConnection(conf);
          try (Connection connection = BigtableConfiguration.connect(conf);
               Admin hBaseAdmin = connection.getAdmin()) {
            while (structuredRecordIterator.hasNext()) {
              StructuredRecord input = structuredRecordIterator.next();
              LOG.info("Received StructuredRecord {}", StructuredRecordStringConverter.toJsonString(input));
              String tableName = CDCHBase.getTableName((String) input.get("table"));
              if (input.getSchema().getRecordName().equals("DDLRecord")) {
                LOG.info("Attempting to create table {}", tableName);
//              CDCHBase.createHBaseTable(hBaseAdmin, tableName);
              } else {
                Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf(tableName));
//              CDCHBase.updateHBaseTable(table, input);
              }
            }
          }
        } finally {
          // Switch back to the original
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }


      }
    });
  }


  public static class CDCBigTableConfig extends ReferencePluginConfig {

    @Name("instance")
    @Description("Instance ID")
    @Macro
    public String instance;
    // TODO: configs


    @Name("project")
    @Description("Project ID")
    @Macro
    public String project;

    @Name("serviceFilePath")
    @Description("Service Account File Path")
    @Macro
    public String serviceAccountFilePath;

    // TODO: why do we need a ctor? (except for unit tests)
    public CDCBigTableConfig(String referenceName) {
      super(referenceName);
    }
  }

}
