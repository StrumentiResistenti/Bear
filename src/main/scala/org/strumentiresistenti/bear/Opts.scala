package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}
import System.err.{println => errln}

object Default {
  val defaultJdbcDriver = "com.cloudera.hive.jdbc4.HS2Driver" /* "org.apache.hive.jdbc.HiveDriver" */
  val defaultSrcUrl = "jdbc:hive2://localhost:10000/"  
  
  def urlCleaner(url: String) = url.replaceAll("/$", "")
}

/**
 * Command line option management
 */
object Dump extends Command(
  name = "dump", 
  description = s"""Schema migration tool for Hive and Impala""".stripMargin
) {
  import Default._
  
  /*
   * Connection parameters
   */
  var srcDriver = cliOpt[String](
    description = s"JDBC driver class for source\n(Default: $defaultJdbcDriver)", 
    default = defaultJdbcDriver)
  
  var srcUrl = cliOpt[String](
    description = s"Source JDBC URL\n(Default: $defaultSrcUrl)", 
    default = defaultSrcUrl)
  
  var srcUrlOpt = cliOpt[String](
    description = "Options to be appended to source URL",
  	default = "AuthMech=0")
  
  /*
   * Authentication parameters
   */
  var srcUser = cliOpt[String](
    description = "Source username",
    default = "hive")
    
  var srcPass = cliOpt[String](
    description = "Source password",
    default = "noPasswordProvided")

  /*
   * Operative parameters
   */
  var database = cliOpt[String](
    description = "Dump this database only\n(Hive's 'default' is implicitly used)",
    default = "default")
  
  var allDatabases = cliOpt[Boolean](
    description = "Dump all databases",
    abbrev = "A")
    
  /*
   * Output
   */
  var output = cliOpt[String](
    description = "Output filename",
    default = "")
    
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
    description = "Add a DROP TABLE IF EXISTS ... before CREATE TABLE",
    abbrev = "T")
    
  var ignorePartitions = cliOpt[Boolean](
    description = "Don't add ALTER TABLE ... CREATE PARTITION statements",
    abbrev = "P")
    
  /*
   * Connection parameters
   */
  var dstDriver = cliOpt[String](
    description = s"JDBC driver class for destination\n(Default: $defaultJdbcDriver)", 
    default = defaultJdbcDriver)
  
  var dstUrl = cliOpt[String](
    description = "Destination JDBC URL", 
    default = "")
  
  var dstUrlOpt = cliOpt[String](
    description = "Options to be appended to destination URL",
  	default = "AuthMech=0")
  
  /*
   * Authentication parameters
   */
  var dstUser = cliOpt[String](
    description = "Destination username",
    default = "hive")
    
  var dstPass = cliOpt[String](
    description = "Destination password",
    default = "noPasswordProvided")
    
  /*
   * Extra methods
   */
  def srcCleanUrl = urlCleaner(srcUrl)
  def srcDbUrl(db: String) = s"${srcCleanUrl}/$db?$srcUrlOpt"
  def srcPureUrl = s"${srcCleanUrl}/$srcUrlOpt"
  
  def dstCleanUrl = urlCleaner(dstUrl)
  def dstDbUrl(db: String) = s"${dstCleanUrl}/$db$dstUrlOpt"
  def dstPureUrl = s"${dstCleanUrl}/$dstUrlOpt" 
}

object Query extends Command(
  name = "query", 
  description = s"""Run queries on Hive and Impala""".stripMargin
) {
  import Default._

  /*
   * Connection parameters
   */
  var driver = cliOpt[String](
    description = s"JDBC driver class for source\n(Default: $defaultJdbcDriver)", 
    default = defaultJdbcDriver)
  
  var url = cliOpt[String](
    description = s"Source JDBC URL\n(Default: $defaultSrcUrl)", 
    default = defaultSrcUrl)
  
  var urlOpt = cliOpt[String](
    description = "Options to be appended to source URL",
  	default = "AuthMech=0")
  
  /*
   * Authentication parameters
   */
  var user = cliOpt[String](
    description = "Source username",
    default = "hive")
    
  var pass = cliOpt[String](
    description = "Source password",
    default = "noPasswordProvided")

  /*
   * Operative parameters
   */
  var database = cliOpt[String](
    description = "Execute query on this database\n(Hive's 'default' is implicitly used)",
    default = "default")
  
  /*
   * Catch the rest of the command line
   */
  var query = args[Seq[String]](description = "SQL query")
  
  /*
   * Extra methods
   */
  def cleanUrl = urlCleaner(url)
  def dbUrl(db: String) = s"${cleanUrl}/$db?$urlOpt"
  def pureUrl = s"${cleanUrl}/$urlOpt"  
}

object Opts {
  def parse(args: Array[String]) = {
    (Cli.parse(args).withCommands(Dump, Query)) getOrElse {
      errln("Unable to parse options")
      sys.exit(1)
    }
  }
}
