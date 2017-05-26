package org.strumentiresistenti.bear

import scalikejdbc._
import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}

/**
 * Command line option management
 */
class Opts extends Command(
  name = "Bear", 
  description = "Schema migration tool for Hive and Impala"
) {
  /*
   * Connection parameters
   */
  var driver = cliOpt[String](
    description = "JDBC driver class to use", 
    default = "com.cloudera.hive.jdbc4.HS2Driver" /* "org.apache.hive.jdbc.HiveDriver" */)
  
  var url = cliOpt[String](
    description = "JDBC URL to connect", 
    default = "jdbc:hive2://localhost:10000/")
  
  var urlOpt = cliOpt[String](
    description = "Options to be appended to JDBC URL",
  	default = "AuthMech=0")
  
  /*
   * Authentication parameters
   */
  var user = cliOpt[String](
    description = "Username to establish the connection",
    default = "hive")
    
  var pass = cliOpt[String](
    description = "Password to establish the connection",
    default = "noPasswordProvided")

  /*
   * Operative parameters 
   */
  var database = cliOpt[String](
    description = "Database to dump",
    default = "default")
  
  var allDatabases = cliOpt[Boolean](
    description = "Dump all databases")
    
  /*
   * Table management parameters
   */
  var ifNotExists = cliOpt[Boolean](
    description = "Add IF NOT EXISTS on every table",
    abbrev = "I")
    
  var dropLocation = cliOpt[Boolean](
    description = "Drop LOCATION on internal tables",
    abbrev = "L")
    
  var dropTable = cliOpt[Boolean](
    description = "Add DROP TABLE IF EXISTS <tablename> before CREATE TABLE",
    abbrev = "D")
    
  var ignorePartitions = cliOpt[Boolean](
    description = "Don't include ALTER TABLE ... CREATE PARTITION statements",
    abbrev = "P")
    
  /*
   * Extra methods
   */
  def cleanUrl = url.replaceAll("/$", "")
  def dbUrl(db: String) = s"${cleanUrl}/$db?$urlOpt"
  def pureUrl = s"${cleanUrl}/?$urlOpt"
}

/**
 * Database types
 */
case class Database(database: String)
object Database {
  def apply(rs: WrappedResultSet): Database = {
    val db = rs.string(1)
    Database(db)
  }
}

case class Table(name: String)
object Table {
  def apply(rs: WrappedResultSet): Table = {
    val name = rs.string(1)
    Table(name)
  }
}

case class TableDDL(ddl: String)
object TableDDL {
  def apply(rs: WrappedResultSet): TableDDL = {
    val ddl = rs.string(1)
    TableDDL(ddl)
  }
}

case class TablePartition(ddl: String)
object TablePartition {
  def apply(rs: WrappedResultSet): TablePartition = {
    val ddl = rs.string(1)
    TablePartition(ddl)
  }
}

/**
 * The application data model
 */
class Bear {
  implicit val session: DBSession = AutoSession
  
  def init(driver: String, url: String, user: String, password: String): Unit = {
    println(s"Provided driver: $driver")
    println(s"Provided URL: $url")
    println(s"Provided user: $user")
    println(s"Provided password: $password")

    Class.forName(driver)
    ConnectionPool.singleton(url, user, password)
  }

  def exec(sql: String): Unit = try {
  	SQL(sql).execute.apply()
  } catch {
    case e: Exception => println(s"ERROR on [$sql]: ${e.getMessage}")
  }
  
  def first[O](sql: String, mapper: (WrappedResultSet) => O): Option[O] = try {
    SQL(sql).map(r => mapper(r)).first.apply()
  } catch {
    case e: Exception => println(s"ERROR on [$sql]: ${e.getMessage}"); None
  }
  
  def list[O](sql: String, mapper: (WrappedResultSet) => O): List[O] = try {
    SQL(sql).map(r => mapper(r)).list.apply()
  } catch {
    case e: Exception => println(s"ERROR on [$sql]: ${e.getMessage}"); Nil
  }
  
  def listConcat[O](sql: String, mapper: (WrappedResultSet) => O)(transform: O => String): String = {
    list(sql, mapper).map(transform).mkString("\n")
  }
  
  def getDatabases: List[Database] = 
    list("show databases", Database(_))
  
  def getTables: List[Table] = 
    list("show tables", Table(_))
  
  def getTableDDL(table: String): String = 
    listConcat(s"show create table $table", TableDDL(_))(_.ddl)
  
  def getTablePartitions(table: String): List[TablePartition] =
    list(s"show partitions $table", TablePartition(_))
}

/**
 * The application controller
 */
object Bear extends App {
  implicit val session: DBSession = AutoSession
  
  val opts = (Cli.parse(args).withCommand(new Opts){case o => o}) getOrElse {
    println("Unable to parse options")
    sys.exit(1)
  }
  
  /*
   * open the connection
   */
  val b = new Bear()
  b.init(opts.driver, opts.pureUrl, opts.user, opts.pass)

  /*
   * do the dump
   */
  if (opts.allDatabases) 
    dumpAllDatabases
  else
    dumpDatabase(opts.database)
    
  /*
   * close and exit
   */
  session.close()
  sys.exit(0)

  /*
   * internal methods
   */
  private def dumpAllDatabases: Unit = {
    val dbs = b.getDatabases.map(_.database) 
    dbs.foreach(dumpDatabase) 
  }

  private def dumpDatabase(db: String): Unit = {
  	b.exec(s"use $db")
    
    val tables = b.getTables.map(_.name)
    
    println(s"""
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
      |--
      |-- Database: $db (${tables.size} tables)
      |--
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --""".stripMargin)
     
    println(s"use $db;")
    tables.foreach(dumpTable)
  }
  
  private def dumpTable(tbl: String): Unit = {
    val ddl = b.getTableDDL(tbl)
    
    if (ddl.toUpperCase().contains("CREATE VIEW")) dumpView(tbl, ddl)
    else {
      println(s"\n--\n-- Table $tbl\n--")
      
      if (opts.dropTable) println(s"DROP TABLE IF EXISTS $tbl;")
      println(s"$ddl;")
      
      /*
       * check if table support partitioning and produce partitions
       */
      if (ddl.toUpperCase().contains("PARTITIONED BY (") && !opts.ignorePartitions) {
        dumpTablePartitions(tbl: String)
      }
    }
  }
  
  private def dumpTablePartitions(tbl: String): Unit = {
    val parts = b.getTablePartitions(tbl)
    parts foreach { p =>
      val tokens = p.ddl.split("/").toList
      val escaped = tokens.map { _.replaceAll("=", "='") + "'" }.mkString(",")
      
      println(s"ALTER TABLE $tbl CREATE PARTITION ($escaped);")
    }
  }
  
  /*
   * TODO: push views in a separate stack 
   * and place them at the end of the file
   */
  private def dumpView(view: String, ddl: String): Unit = {
    println(s"\n--\n-- View: $view\n--")
    
    if (opts.dropTable) println(s"DROP VIEW IF EXISTS $view;")
    println(s"$ddl;")
  }
}