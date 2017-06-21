CDC Database Source
===================

CDAP plugin for reading from the Kafka topic to which Oracle GoldenGate pushes the change schema and data.
This plugin is used in realtime data pipelines only.

Usage Notes
-----------
The plugin is configured to pull the changes from the Kafka topic to which Oracle GoldenGate pushes the change schema
and data. Note that the kafka topic should have one partition and GoldenGate should be configured to emit the schema as
well as data to the same topic. This guarantees the ordering of the events as they occurs in the source database.
This plugin can be used along with CDC sink plugins such as `CDC Kudu`, and `CDC HBase`.


Plugin Configuration
---------------------
| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Y** | N/A | A unique name which will be used to identify this source for lineage and annotating metadata.|
| **Kafka Broker** | **Y** | N/A | Kafka broker specified in host:port form. For example, example.com:9092  |
| **Kafka Topic** | **Y** | N/A | Name of the topic to which Oracle GoldenGate is configured to publish schema and data changes. This topic should have single partition to maintain the ordering between the published messages. |
| **Default Initial Offser** | **N** | -1 | The default initial offset for all topic partitions. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. |
| **Max Rate Per Partition** | **N** | 1000 | Maximum number of records to read per second per partition. 0 means there is no limit. Defaults to 1000. |
