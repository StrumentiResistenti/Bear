package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}

object Query extends Command(
  name = "query", 
  description = s"""Run queries on Hive and Impala""".stripMargin
) with CommandRunner {
  import OptsDefault._

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

  override def run: Unit = {
    val src = new Bear(driver, pureUrl, user, pass)
    if (database != "") src.exec(s"use ${database}")
    src.arbitraryQuery(query.mkString(" "))
  }
}

