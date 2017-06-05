# Bear
Hive dump tool

Bear is the evolution of HiveDump. It's a complete rewrite of the tool using Scala. The basic idea is to use JDBC to query a source database to fetch its metadata and the emit them on a selectable channel: standard output, a file or another JDBC connection. Some transformations are allowed during the process, like removing the `LOCATION`, adding a `DROP TABLE/VIEW IF EXISTS` before each `CREATE`, add the `IF NOT EXISTS` clause before each table or view and drop `ALTER TABLE .. CREATE PARTITION` statements.

A `query` subcommand is provided to ease the metadata export. Query results are output on standard output. This feature is supposed to be used for queries like `show table` or `show partitions <table>`.

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
