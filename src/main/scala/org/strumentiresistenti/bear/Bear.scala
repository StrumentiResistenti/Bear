package org.strumentiresistenti.bear

import scalikejdbc._
import System.err.{println => errln}

/**
 * The application data model
 */
class Bear {
  implicit val session: DBSession = AutoSession
  
  def init(driver: String, url: String, user: String, password: String): Unit = {
    errln(s"Provided driver: $driver")
    errln(s"Provided URL: $url")
    errln(s"Provided user: $user")
    errln(s"Provided password: $password")

    Class.forName(driver)
    ConnectionPool.singleton(url, user, password)
  }

  def exec(sql: String): Unit = try {
  	SQL(sql).execute.apply()
  } catch {
    case e: Exception => errln(s"ERROR on [$sql]: ${e.getMessage}")
  }
  
  def first[O](sql: String, mapper: (WrappedResultSet) => O): Option[O] = try {
    SQL(sql).map(r => mapper(r)).first.apply()
  } catch {
    case e: Exception => errln(s"ERROR on [$sql]: ${e.getMessage}"); None
  }
  
  def list[O](sql: String, mapper: (WrappedResultSet) => O): List[O] = try {
    SQL(sql).map(r => mapper(r)).list.apply()
  } catch {
    case e: Exception => errln(s"ERROR on [$sql]: ${e.getMessage}"); Nil
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
  
  val dropLocationRx = """LOCATION\n\s+'[^']+'\n""".r
  
  /*
   * parse command line
   */
  val opts = Opts.parse(args)
  
  /**
   * Emit buffer and output channel
   */
  var buffer = ""
  var outputFile: java.io.File = 
    if (opts.output.length > 0) new java.io.File(opts.output) else null
  
  /*
   * open the connection
   */
  val b = new Bear()
  b.init(opts.driver, opts.pureUrl, opts.user, opts.pass)

  /*
   * do the dump
   */
  if (opts.allDatabases) dumpAllDatabases 
  else dumpDatabase(opts.database)
    
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
  	
  	TableDeps.clean
  	buffer = ""
    
    val tables = b.getTables.map(_.name)
    
    comment(s"""
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
      |--
      |-- Database: $db (${tables.size} tables/views)
      |--
      |-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --""".stripMargin)
     
    emit(s"use $db;")
    tables.foreach(t => dumpTable(t, db))
    
    dumpUpperDependencies
  }
  
  private def dumpTable(tbl: String, db: String): Unit = {
    val ddl = b.getTableDDL(tbl)
    
    if (ddl.toUpperCase().contains("CREATE VIEW")) {
      TableDeps.addViewCandidate(tbl, db, ddl)
    } else {
      TableDeps.addTable(tbl)

      comment(s"\n--\n-- Table $tbl\n--")

      if (opts.dropTable) emit(s"DROP TABLE IF EXISTS $tbl;\n")
      
      if (opts.dropLocation) 
        emit(dropLocationRx.replaceAllIn(s"$ddl;", ""))
      else 
        emit(s"$ddl;")

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
      
      emit(s"ALTER TABLE $tbl CREATE PARTITION ($escaped);")
    }
  }
  
  private def dumpUpperDependencies: Unit = {
    /*
     * 1. resolve views in the right order
     */
    TableDeps.resolveViewCandidates    
    
    /*
     * 2. dump the 
     */
    val vs = (TableDeps.deps - 0).toList.sortBy{ case (i, _) => i }
    
    comment(s"\n--\n-- Dumping ${vs.size} views\n--\n")
    vs foreach { case (_, views) =>
      views foreach { s =>
        val ddl = b.getTableDDL(s)
        dumpView(s, ddl)
      }
    }
  }
  
  private def dumpView(view: String, ddl: String): Unit = {
    comment(s"\n--\n-- View: $view\n--")
    
    if (opts.dropTable) emit(s"DROP VIEW IF EXISTS $view;")
    emit(s"$ddl;")
  }
  
  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  
  /**
   * Receive a new line of output and buffer it, or forward it
   * to destination, or dump it on stdout
   */
  def emit(s: String, comment: Boolean = false): Unit = {
    buffer = buffer + s
    
    if (opts.output.length != 0) println(s)
    else printToFile(outputFile)(_.println(s))
  }
  
  def comment(s: String) = emit(s, true)
}