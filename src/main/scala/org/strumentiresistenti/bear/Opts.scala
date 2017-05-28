package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}
import System.err.{println => errln}

/**
 * Command line option management
 */
class Opts extends Command(
  name = "Bear", 
  description = s"""Schema migration tool for Hive and Impala
    |
    | (c) 2017 Tx0 <tx0@strumentiresistenti.org>""".stripMargin
) {
  /*
   * Connection parameters
   */
  var driver = cliOpt[String](
    description = "JDBC driver class for source", 
    default = "com.cloudera.hive.jdbc4.HS2Driver" /* "org.apache.hive.jdbc.HiveDriver" */)
  
  var url = cliOpt[String](
    description = "Source JDBC URL", 
    default = "jdbc:hive2://localhost:10000/")
  
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
    description = "Add DROP TABLE IF EXISTS <tablename> before CREATE TABLE",
    abbrev = "T")
    
  var ignorePartitions = cliOpt[Boolean](
    description = "Don't add ALTER TABLE ... CREATE PARTITION statements",
    abbrev = "P")
    
  /*
   * Connection parameters
   */
  var dstDriver = cliOpt[String](
    description = "JDBC driver class for destination", 
    default = "com.cloudera.hive.jdbc4.HS2Driver")
  
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

  private def urlCleaner(url: String) = url.replaceAll("/$", "")
  /*
   * Extra methods
   */
  def cleanUrl = urlCleaner(url)
  def dbUrl(db: String) = s"${cleanUrl}/$db?$urlOpt"
  def pureUrl = s"${cleanUrl}/$urlOpt"
  
  def dstCleanUrl = urlCleaner(dstUrl)
  def dstDbUrl(db: String) = s"${dstCleanUrl}/$db$urlOpt"
  def dstPureUrl = s"${dstCleanUrl}/$urlOpt" 
}

object Opts {
  def parse(args: Array[String]) = {
    (Cli.parse(args).withCommand(new Opts){case o => o}) getOrElse {
      errln("Unable to parse options")
      sys.exit(1)
    }
  }
}
