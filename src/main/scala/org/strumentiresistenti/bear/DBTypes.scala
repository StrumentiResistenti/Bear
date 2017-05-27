package org.strumentiresistenti.bear

import scalikejdbc._

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
