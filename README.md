# Bear
A Hive (metadata) dump tool

Bear is the evolution of HiveDump. It's a complete rewrite of the tool using Scala. The basic idea is to use JDBC to query a source database to fetch its metadata and then emit them on a selectable channel: standard output, a file or another JDBC connection. Some transformations are allowed during the process, like removing the `LOCATION` from tables, adding a `DROP TABLE/VIEW IF EXISTS` before each `CREATE`, add the `IF NOT EXISTS` clause before each table or view and drop `ALTER TABLE .. CREATE PARTITION` statements, which are by default produced.

A `query` subcommand is provided to ease the metadata export. Query results are output on standard output without any particular formatting. This feature is supposed to be used for queries like `show table` or `show partitions <table>`.

```
Usage

 bear [options] command [command options]

Commands

   dump [command options] : Schema migration tool for Hive and Impala
      -A, --all-databases     : Dump all databases
      --database=STRING       : Dump this database only
                                (Hive's 'default' is implicitly used)
      -L, --drop-location     : Drop LOCATION on internal tables
      -T, --drop-table        : Add a DROP TABLE IF EXISTS ... before CREATE TABLE
      --dst-driver=STRING     : JDBC driver class for destination
                                (Default: com.cloudera.hive.jdbc4.HS2Driver)
      --dst-pass=STRING       : Destination password
      --dst-url=STRING        : Destination JDBC URL
      --dst-url-opt=STRING    : Options to be appended to destination URL
      --dst-user=STRING       : Destination username
      -I, --if-not-exists     : Add IF NOT EXISTS on every table
      -P, --ignore-partitions : Don't add ALTER TABLE ... CREATE PARTITION statements
      --output=STRING         : Output filename
      --src-driver=STRING     : JDBC driver class for source
                                (Default: com.cloudera.hive.jdbc4.HS2Driver)
      --src-pass=STRING       : Source password
      --src-url=STRING        : Source JDBC URL
                                (Default: jdbc:hive2://localhost:10000/)
      --src-url-opt=STRING    : Options to be appended to source URL
      --src-user=STRING       : Source username

   query [command options] <query> ... : Run queries on Hive and Impala
      --database=STRING : Execute query on this database
                          (Hive's 'default' is implicitly used)
      --driver=STRING   : JDBC driver class for source
                          (Default: com.cloudera.hive.jdbc4.HS2Driver)
      --pass=STRING     : Source password
      --url=STRING      : Source JDBC URL
                          (Default: jdbc:hive2://localhost:10000/)
      --url-opt=STRING  : Options to be appended to source URL
      --user=STRING     : Source username
      <query> : SQL query

No command found, expected one of dump, query
```
Build instructions
------------------
Bear is built with SBT. To build it:

```
$ sbt
[info] Loading global plugins from /home/tx0/.sbt/0.13/plugins
[info] Loading project definition from /home/tx0/scala_workspace/bear/project
[info] Set current project to bear (in build file:/home/tx0/scala_workspace/bear/)
> compile
[success] Total time: 0 s, completed Jun 6, 2017 12:05:16 AM
> assembly
[...more output here...]
> exit
$ sudo cp target/scala-2.11/bear-assembly-0.1.jar /usr/local/lib/
```
Then create a file named `/usr/local/bin/bear` with this content:

```
#!/bin/sh

java -jar /usr/local/lib/bear-assembly-0.1.jar $@
```
A better install procedure will be provided somewhere in the future.

How to pack an arbitrary JDBC driver
------------------------------------
Create a directory called `lib/` in the root directory of the project and copy all required jars inside it. Cloudera driver can be packed by providing the following jars in `lib/`:

```
$ ls lib/
HiveJDBC4.jar
TCLIServiceClient.jar
hive_metastore.jar
hive_service.jar
libfb303-0.9.0.jar
libthrift-0.9.0.jar
ql.jar
```

When the jars are ready, just re-execute `sbt assembly` and copy the `bear-assembly-0.1.jar` again.

Why bear?
---------

Partly because a bear is a *hive* plunderer and partly because this tool *bears* things (metadata) to another place. Got the pun?
