Golden Gate Streaming Source
==========

CDAP Plugin that reads from a Golden Gate Kafka topic. Plugin can be configured only for real-time pipelines  

Usage Notes
-----------

The output of this plugin contains raw DDL and DML records which specify the actions needed to update the database. A normalizer plugin is usually placed after this plugin to extract and wrap the outputs in a `StructuredRecord`.

Plugin Configuration
---------------------

The following configurations collectively specify a Kafka consumer that allows the plugin to pull data from a specific Kafka topic

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Yes** | N/A | Name of the plugin instance.| 
| **Kafka Broker** | **Yes** | N/A | Specifies the address of the kafka broker. Must be in `host:port` format |
| **Default Initial Offset** | **No** | -1 | Offset of the message to read from. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. |
| **Max Rate Per Partition** | **No** | 1000 | Maximum number of records to read per second in each partition. |

Note that this plugin only supports a Golden Gate source with one partition.

Build
-----
To build this plugin:

```
   mvn clean package -DskipTests
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'CDCHBase-sink-1.0.0':

    > load artifact target/CDCHBase-sink-1.0.0.jar config-file target/CDCHBase-sink-1.0.0.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.    
