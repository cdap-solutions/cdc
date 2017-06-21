CDC Kudu Sink
=============

CDAP plugin for storing the changed data into Kudu table. This plugin can be used with
either `CDC Database Source` or `ChangeTrackingSQLServer Source` plugin for
reading the changes from Oracle or SQLServer databases respectively.

Usage Notes
-----------
The plugin accepts the DDL and DML records from the source plugin to which it is connected to. If the table does not
exists in the Kudu cluster, it will be created. DDL operations such as `add column` or `remove column` will
result in the corresponding schema changes in the Kudu table. DML operations such as `Insert`, `Update`, or `Delete`
results into updates of the corresponding record in the Kudu table.

Plugin Configuration
--------------------

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Y** | N/A | A unique name which will be used to identify this source for lineage and annotating metadata.|
| **Kudu Master Host** | **Y** | N/A | Specifies the list of Kudu master hosts that this plugin will attempt connect to. It's a comma separated list of &lt;hostname&gt;:&lt;port&gt;. Connection is attempt after the plugin is initialized in the pipeline.  |
| **No of Buckets** | N | 16 | Number of buckets the keys are split into |
| **Hash seed** | N | 1 | The seed value specified is used to randomize mapping of rows to hash buckets. Setting the seed will ensure the hashed columns contain user provided values.|
| **Compression Algorithm** | N | Snappy | Specifies the compression algorithm to be used for the columns. Following are different options available. |
| **Encoding** | N | Auto Encoding | Specifies the block encoding for the column. Following are different options available.  |
| **Operation Timeout** | N | 30000 | This configuration sets the timeout in milliseconds for user operations with Kudu. If you are writing large sized records it's recommended to increase the this time. It's defaulted to 30 seconds. |
| **Admin Timeout** | N | 30000 | This configuration is used to set timeout in milliseconds for administrative operations like for creating table if table doesn't exist. This time is mainly used during initialize phase of the plugin when the table is created if it doesn't exist. |
| **Number of replicas** | N | 1 | Specifies the number of replicas for the above table. This will specify the number of replicas that each tablet will have. By default it will use the default set on the server side and that is generally 1.|
| **Rows to be cached** | N | 1000 | Specifies number of rows to be cached before being flushed |
| **Boss Threads** | N | 1 | Number of boss threads used in the Kudu client to interact with Kudu backend. |

