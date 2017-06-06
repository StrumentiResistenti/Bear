/*
   Bear -- A Hive metadata export tool -- Bear.scala
   Copyright (C) 2017 Tx0 <tx0@strumentiresistenti.org>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA. 
*/

package org.strumentiresistenti.bear

import scalikejdbc._
import System.err.{println => errln}
import java.sql.ResultSetMetaData
import org.backuity.clist.Command

/**
 * The application data model
 */
class Bear(val driver: String, val url: String, val user: String, val password: String) {
  implicit val session: DBSession = AutoSession
  
  errln(s"Provided driver: $driver")
  errln(s"Provided URL: $url")
  errln(s"Provided user: $user")
  errln(s"Provided password: ${password.replaceAll(".", "*")}")

  Class.forName(driver)
  ConnectionPool.singleton(url, user, password)

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
  
  /*
   * Produce a recordPrinter function to print data set records
   */
  private def recordPrinterFactory: WrappedResultSet => Unit = {
    var printedLabel = false
    def recordPrinter(r: WrappedResultSet): Unit = {
      val nCols = r.metaData.getColumnCount
      if (!printedLabel) {
        val header = "| " + (1 to nCols).map { i => r.metaData.getColumnLabel(i) }.mkString(" | ") + " |"
        println(header)
        println("-" * header.length)
        printedLabel = true
      }
      println("| " + (1 to nCols).map { i => r.any(i).toString() }.mkString(" | ") + " |")
    }
    recordPrinter
  }
  
  /*
   * Execute an arbitrary query
   */
  def arbitraryQuery(sql: String): Unit = {
    try {
      val query = SQL(sql)
      query.foreach(recordPrinterFactory)
    } catch {
      case e: Exception =>
        println(s"Error on [$sql]: ${e.getMessage}")
    }
  }
}

/**
 * The application controller
 */
object Bear extends App {
  implicit val session: DBSession = AutoSession
  
  /*
   * parse command line and execute the command
   */
  Opts.parse(args) match {
    case c: CommandRunner => c.run
  }
  
  /*
   * close and exit
   */
  session.close()
  sys.exit(0)
}
