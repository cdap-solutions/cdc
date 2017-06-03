# CDC Database Sink


Description
-----------
Writes CDC records to Database using JDBC. It supports creating and writing to multiple tables based on change records.


Use Case
--------
The sink is used to propogate CDC records pushed by goldengate and write them transactionally to database in realtime.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication. (Macro-enabled)

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication. (Macro-enabled)

**jdbcPluginName:** Name of the JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**jdbcPluginType:** Type of the JDBC plugin to use. This is the value of the 'type' key
defined in the JSON file for the JDBC plugin. Defaults to 'jdbc'.

**connectionString:** JDBC connection string including database name. (Macro-enabled)

**outputschema:** The schema of records output by the source. This will be used in place of whatever schema comes 
back from the query. However, it must match the schema that comes back from the query, 
except it can mark fields as nullable and can contain a subset of the fields. 

Example
-------
This example connects to a database using the specified 'connectionString', which means
it will connect to the 'demo' database of a SQL Server instance running on 'ip' and 'port'.
Each input record will be written to corresponding database tables.

    {
        "name": "CDCDatabaseSink",
        "type": "batchsink",
        "properties": {
            "referenceName": "CDCDatabaseSink",
            "connectionString": "jdbc:sqlserver://<ip>:<port>;databaseName=demo;user=user;password=password;",
            "jdbcPluginName": "sqlserver",
            "jdbcPluginType": "jdbc",
            "outputschema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"EMPNO\",\"type\":\"long\"},{\"name\":\"ENAME\",\"type\":\"string\"},
            {\"name\":\"JOB\",\"type\":\"string\"},{\"name\":\"MGR\",\"type\":\"long\"},{\"name\":\"HIREDATE\",\"type\":\"string\"},{\"name\":\"SAL\",\"type\":\"long\"},
            {\"name\":\"COMM\",\"type\":\"long\"},{\"name\":\"DEPTNO\",\"type\":\"long\"}]}",
        }
    }
