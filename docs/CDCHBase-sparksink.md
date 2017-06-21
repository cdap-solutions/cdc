CDC HBase Sink
==============

CDAP plugin for storing the changed data into HBase. This plugin can be used with
either `CDC Database Source` or `ChangeTrackingSQLServer Source` plugin for reading the changes
from Oracle or SQLServer databases respectively.

Usage Notes
-----------

The plugin accepts the DDL and DML records from the source plugin to which it is connected to. If the table does not
exists in the HBase cluster, it will be created.

Plugin Configuration
---------------------

This plugin only accepts the reference name. Other configurations required to connect to the HBase cluster are
picked up from hbase-site.xml.

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Y** | N/A | A unique name which will be used to identify this source for lineage and annotating metadata.|
