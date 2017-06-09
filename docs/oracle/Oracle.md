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

* [CDC Database Source](../CDCDatabase-source.md)
* [CDC Kudu Sink](../CDCKudu-sink.md)
* [CDC HBase Sink](../CDCHBase-sink.md)


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


Downloading the Oracle GoldenGate software
-------------------------------------------

We need two GoldenGate processes running. Standard Oracle GoldenGate process which acts as EXTRACT.
This process integrates with the Oracle database server, read the changed data and writes to the
trail file. Another process is Oracle GoldenGate for Big Data which is responsible for publishing the
changes to the big data systems such as Kafka.

**[Download](http://www.oracle.com/technetwork/middleware/goldengate/downloads/index.html)** the
standard Oracle GoldenGate and follow the instructions in section 2.5.2 as given
**[here](https://docs.oracle.com/goldengate/1212/gg-winux/GIORA/install.htm#GIORA162)**.

From the instructions, you’ll need to create a response file (oggcore.rsp).
We used following properties in the response file while testing:

    INSTALL_OPTION=ORA12c
    SOFTWARE_LOCATION=/u01/ogg
    START_MANAGER=true
    MANAGER_PORT=9999
    DATABASE_LOCATION=/u01/app/oracle/product/12.1.0/xe
    UNIX_GROUP_NAME=dba

Execute the `runInstaller` binary passing the name of the response file created above as argument:

    cd fbo_ggs_Linux_x64_shiphome/Disk1
    ./runInstaller -silent -nowait -responseFile /u01/oggcore.rsp

**[Download](http://www.oracle.com/technetwork/middleware/goldengate/downloads/index.html)** the
Oracle GoldenGate for Big Data 12.3.0.1.0 and unzip the downloaded file at location say `/u01/ogg-bd`.


Configuring the GoldenGate Manager
----------------------------------
Manager in GoldenGate is responsible for managing the `EXTRACT` and `REPLICAT` processes.

Login to the GoldenGate command interface(ggsci):

    cd /u01/ogg
    ./ggsci

If you receive following error:

    ./ggsci: error while loading shared libraries: libnnz12.so: cannot open shared object file: No such file or directory

This means that the GoldenGate is not able to find the Oracle installation, source the oracle environment:

    . oraenv

Please enter the location of the Oracle database when prompted. The location can be typically
found in the file `/etc/oratab`.

At the GGSCI prompt create required subdirs which will store the data and configurations for the GoldenGate processes:

    create subdirs

Enter `EDIT PARAM MGR` and in the resulting vi edit session add:

    DynamicPortList 20000-20099
    PurgeOldExtracts ./dirdat/*, UseCheckPoints, MinKeepHours 2
    Autostart Extract E*
    AUTORESTART Extract *, WaitMinutes 1, Retries 3

Save and exit vi and start the mgr process:

    start mgr

Make sure manager is running:

    GGSCI (bigdatalite.localdomain) 1> info mgr
    Manager is running (IP port bigdatalite.localdomain.7811, Process ID 23018).

Configure the manager for the Oracle GoldenGate for big data:

    cd /u01/ogg-bd/
    ./ggsci

At the GGSCI prompt `EDIT PARAM MGR` and check that it is set to `PORT 7810`.

Close the `vi` session and start the manager:

    GGSCI (bigdatalite.localdomain) 3> start mgr
    Manager started.
    GGSCI (bigdatalite.localdomain) 4> info mgr
    Manager is running (IP port bigdatalite.localdomain.7810, Process ID 24416).

Define GoldenGate EXTRACT
-------------------------

Return to the standard GoldenGate installation in `/u01/ogg`. Assuming database instance is `xe.oracle.docker`,
login to instance through GGSCI:

    DBLOGIN USERID ggtest@localhost:1521/xe.oracle.docker PASSWORD ggtest
	  ADD SCHEMATRANDATA GGTEST ALLCOLS

Register the integrated EXTRACT process:

	  DBLOGIN USERID GGTEST PASSWORD ggtest
	  REGISTER EXTRACT EXT1 DATABASE

Define the EXTRACT:

    ADD SCHEMATRANDATA GGTEST
	  ADD EXTRACT EXT1, INTEGRATED TRANLOG, BEGIN NOW

Write a trail file for EXTRACT:

	  ADD EXTTRAIL ./dirdat/lt EXTRACT EXT1

Specify EXTRACT parameters:

    EDIT PARAM EXT1

    In edit session add following parameters

    EXTRACT EXT1
    USERID GGTEST, PASSWORD ggtest
    EXTTRAIL ./dirdat/lt
    DDL INCLUDE ALL
    DDLOPTIONS ADDTRANDATA, REPORT
    GETTRUNCATES
    TABLE GGTEST.*,
    GETBEFORECOLS (
    ON UPDATE ALL,
    ON DELETE ALL );

    Save and close the file.

Start EXTRACT EXT1 and make sure it is running:

    START EXT1
    INFO EXT1

    If you get STARTING then re-issue command till RUNNING - may take few minutes.
    If you get ABENDED check /u01/ogg/ggserr.log for the details.

At this point you should see that the file is created as:

    [oracle@bigdatalite dirdat]$ ls -l /u01/ogg/dirdat/
    total 4
    -rw-r-----. 1 oracle oinstall 1406 Sep  2 14:46 lt000000000

Define another extract in GGSCI(`/u01/ogg`) session. This will act as pump process to copy the trail files to remote:

    ADD EXTRACT EXTDP1 EXTTRAILSOURCE ./dirdat/lt BEGIN NOW
    ADD RMTTRAIL ./dirdat/rt EXTRACT EXTDP1

Edit parameters for the EXTDP1:

    EDIT PARAM EXTDP1

    In edit session add following parameters

    EXTRACT EXTDP1
    RMTHOST LOCALHOST, MGRPORT 7810
    RMTTRAIL ./dirdat/rt
    PASSTHRU
    DDL INCLUDE ALL
    GETTRUNCATES
    TABLE GGTEST.*;

    Note that MGRPORT must match the OGG big data manager port.
    Save and close the vi session.

Start EXTRACT and make sure it is running fine:

    START EXTDP1
    INFO EXTDP1

In `ogg-bd`, remote trail file should be created by now:

    oracle@bigdatalite ogg]$ ls -l /u01/ogg-bd/dirdat/
    total 0
    -rw-r-----. 1 oracle oinstall 0 Sep  2 15:02 rt000000000

    If the results are not as expected check the ggserr.log in both /u01/ogg and /u01/ogg-bd directories.


Configuring Kafka Handler in GoldenGate big data
------------------------------------------------

Please make sure that Java and **[Kafka client libraries](https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-1C21BC19-B3E9-462C-809C-9440CAB3A427.htm#GADBD373)**
are installed on the server where GoldenGate big data processes are running.

`REPLICAT` process loads JVM dynamically for which it needs `libjvm.so` and `libjsig.so` to load at runtime.
These library files are shipped with Java, so we need to update the `LD_LIBRARY_PATH` as:

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<JAVA_HOME>/jre/lib/amd64/server

Restart the Manager on OGG big data using GGSCI prompt:

    cd /u01/ogg-bd/
    ./ggsci
    stop mgr
    start mgr
    info mgr

Configure the REPLICAT process which will push the changes in the trail file to Kafka:

    edit param rconf

    Now in vi session add following lines

    REPLICAT rconf
    TARGETDB LIBFILE libggjava.so SET property=dirprm/conf.props
    REPORTCOUNT EVERY 1 MINUTES, RATE
    GROUPTRANSOPS 1000
    DDLOPTIONS REPORT
    DDL INCLUDE ALL
    GETTRUNCATES
    MAP *.*, TARGET *.*;

    Save and close the vi session.

Create the handler configuration file (/u01/ogg-bd/dirprm/conf.props) file.
This file defines the properties for Kafka handler which are documented **[here](https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-2561CA12-9BAC-454B-A2E3-2D36C5C60EE5.htm#GADBD455)**.

Sample `conf.props` file:

    gg.handlerlist = kafka
    gg.handler.kafkahandler.Type = kafka
    gg.handler.kafka.KafkaProducerConfigFile = custom_kafka_producer.properties
    gg.handler.kafka.Format = avro_op
    gg.handler.kafka.TopicName =dbcdc
    gg.handler.kafka.SchemaTopicName =dbcdc
    gg.handler.kafka.Mode = op
    gg.handler.kafka.format.wrapMessageInGenericAvroMessage =true
    goldengate.userexit.timestamp=utc
    goldengate.userexit.writers=javawriter
    javawriter.stats.display=TRUE
    javawriter.stats.full=TRUE
    gg.log=log4j
    gg.log.level=TRACE
    gg.report.time=30sec
    #Set the classpath here
    gg.classpath=dirprm/:/u01/app/oracle/gg_bigdata/ggjava/resources/lib/*:/u01/app/oracle/kafka/kafka_2.10-0.9.0.0/libs/*
    javawriter.bootoptions=-Xmx512m -Xms32m -Djava.class.path=.:ggjava/ggjava.jar:./dirprm


**Please note that schema and data should go to the same Kafka topic and this topic should have one partition only.**

Provide the Kafka producer configurations. Note that based on the handler properties above the name of the config
file is `custom_kafka_producer.properties`. This file should be created in the `dirprm` directory. Please see the
sample Kafka produces configurations provided **[here](https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-2561CA12-9BAC-454B-A2E3-2D36C5C60EE5.htm#GADBD459)**.

Now we can add replicat from GGSCI prompt and start:

    ADD REPLICAT RCONF, EXTTRAIL ./dirdat/rt
    START RCONF
    info RCONF

Verify the setup
================

Execute INSERT/UPDATE/DELETE operations on the employee table in the GGTEST schema.

Using Kafka console consumer, read the data from topic dbcdc and make sure that the updates are available in Kafka.
