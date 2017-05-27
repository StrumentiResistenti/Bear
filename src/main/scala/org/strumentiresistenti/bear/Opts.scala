package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}
import System.err.{println => errln}

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

object Opts {
  def parse(args: Array[String]) = {
    (Cli.parse(args).withCommand(new Opts){case o => o}) getOrElse {
      errln("Unable to parse options")
      sys.exit(1)
    }
  }
}
