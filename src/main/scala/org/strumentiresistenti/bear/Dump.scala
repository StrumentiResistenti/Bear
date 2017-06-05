package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}

/**
 * Command line option management
 */
object Dump extends Command (
  name = "dump", 
  description = s"""Schema migration tool for Hive and Impala""".stripMargin
) with CommandRunner {
  import OptsDefault._
  
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
  
  import Emitter.{comment, emit, emitBuffer}

  override def run: Unit = {
    val src = new Bear(Dump.srcDriver, Dump.srcPureUrl, Dump.srcUser, Dump.srcPass)
  
    /*
     * do the dump
     */
    Emitter.init
    if (Dump.allDatabases) dumpAllDatabases(src)
    else dumpDatabase(src, Dump.database)    
  }
  
  /*
   * internal methods
   */
  private def dumpAllDatabases(src: Bear): Unit = {
    val dbs = src.getDatabases.map(_.database) 
    dbs.foreach(d => dumpDatabase(src, d)) 
  }

  private def dumpDatabase(src: Bear, db: String): Unit = {
  	src.exec(s"use $db")
  	
  	TableDeps.clean
  	Emitter.reset
    
    val tables = src.getTables.map(_.name)
    
    comment(s"""
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
      |--
      |-- Database: $db (${tables.size} tables/views)
      |--
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --""".stripMargin)
     
    emit(s"use $db")
    tables.foreach(t => dumpTable(src, t, db))
    
    dumpUpperDependencies(src)
	  emitBuffer
  }
  
  val dropLocationRx = """LOCATION\n\s+'[^']+'\n""".r
  
  private def dumpTable(src: Bear, tbl: String, db: String): Unit = {
    val ddl = src.getTableDDL(tbl)
    
    if (ddl.toUpperCase().contains("CREATE VIEW")) {
      TableDeps.addViewCandidate(tbl, db, ddl)
    } else {
      TableDeps.addTable(tbl)

      comment(s"\n--\n-- Table $tbl\n--")

      if (Dump.dropTable) emit(s"DROP TABLE IF EXISTS $tbl\n")
      
      if (Dump.dropLocation) 
        emit(dropLocationRx.replaceAllIn(s"$ddl", ""))
      else 
        emit(s"$ddl")

      /*
       * check if table support partitioning and produce partitions
       */
      if (ddl.toUpperCase().contains("PARTITIONED BY (") && !Dump.ignorePartitions) {
        dumpTablePartitions(src, tbl)
      }
    }
  }
  
  private def dumpTablePartitions(src: Bear, tbl: String): Unit = {
    val parts = src.getTablePartitions(tbl)
    parts foreach { p =>
      val tokens = p.ddl.split("/").toList
      val escaped = tokens.map { _.replaceAll("=", "='") + "'" }.mkString(",")
      
      emit(s"ALTER TABLE `$tbl` CREATE PARTITION ($escaped)")
    }
  }
  
  private def dumpUpperDependencies(src: Bear): Unit = {
    /*
     * 1. resolve views in the right order
     */
    TableDeps.resolveViewCandidates    
    
    /*
     * 2. dump the 
     */
    val vs = (TableDeps.deps - 0).toList.sortBy{ case (i, _) => i }
    
    comment(s"\n--\n-- Dumping resolved views\n--")
    vs foreach { case (_, views) =>
      views foreach { s =>
        val ddl = src.getTableDDL(s)
        comment(s"\n--\n-- View: $s\n--")

        if (Dump.dropTable) emit(s"DROP VIEW IF EXISTS $s")
        emit(ddl)
      }
    }
    
    /*
     * 3. Record unresolved dependencies
     */
    TableDeps.rejectedViews foreach { rej =>
      val txt = rej.ddl.replaceAll("\n", "\n--  ")
      comment(s"\n--\n-- Unresolved view ${rej.view}\n--\n--  $txt")
    }
  }  
}

