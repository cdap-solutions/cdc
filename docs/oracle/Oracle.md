Change Data Capture(CDC) for Oracle
===================================

Oracle GoldenGate is a realtime Change Data Capture system which allows streaming
changes from the Oracle database to a target system. We configure the Oracle GoldenGate
to stream all changes made to a table, or a set of tables to a Kafka. These changes are then
read by CDAP realtime data pipeline making them available in the Hadoop ecosystem, such as Kudu,
HBase etc. Once the data is in Hadoop, computations can be performed on the data with low cost.

Following document explains the steps required to setup the GoldenGate processes with the Oracle database

* [Setting up GoldenGate](GoldenGateSetup.md)

Once GoldenGate is configured to stream changes to a Kafka topic, following CDAP plugins can be used to import
those changes in Hadoop.

* [CDC Database Source](../CDCDatabase.md)
* [CDC Kudu Sink](../CDCKudu.md)
* [CDC HBase Sink](../CDCHBase.md)


Setup the GoldenGate for Oracle Change Data Capture
===================================================

Rest of the document provides the instructions to setup GoldenGate with Oracle database.
Please note that the instructions might need some tweaks depending on the environment.


Prepare database for the Change Data Capture
--------------------------------------------

Login to database as sysdba:

    sqlplus / as sysdba

Execute the following commands to enable supplemental logging on the database:

    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE FORCE LOGGING;
    SHUTDOWN IMMEDIATE
    STARTUP MOUNT
    ALTER DATABASE ARCHIVELOG;
    ALTER DATABASE OPEN;
    ALTER SYSTEM SWITCH LOGFILE;
    ALTER SYSTEM SET ENABLE_GOLDENGATE_REPLICATION=TRUE SCOPE=BOTH;
    EXIT

Create GoldenGate test user(ggtest):

    create user ggtest identified by ggtest;
    grant resource, dba, connect to ggtest;

Exit sqlplus and start the Oracle listener controller if it is not running already:

    lsnrctl start
    lsnrctl status


Setting up GoldenGate processes
-------------------------------

We need two GoldenGate processes running. Standard Oracle GoldenGate process which acts as EXTRACT.
This process integrates with the Oracle database server, read the changed data and writes to the
trail file. Another process is Oracle GoldenGate for Big Data which is responsible for publishing the
changes to the big data systems such as Kafka.


CDCDatabase Source
------------------

Plugin Configuration
---------------------
| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Y** | N/A | A unique name which will be used to identify this source for lineage and annotating metadata.|
| **Kafka Broker** | **Y** | N/A | Kafka broker specified in host:port form. For example, example.com:9092  |
| **Kafka Topic** | **Y** | N/A | Name of the topic to which Oracle GoldenGate is configured to publish schema and data changes. This topic should have single partition to maintain the ordering between the published messages. |
| **Default Initial Offser** | **N** | -1 | The default initial offset for all topic partitions. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. |
| **Max Rate Per Partition** | **N** | 1000 | Maximum number of records to read per second per partition. 0 means there is no limit. Defaults to 1000. |
