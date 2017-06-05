package org.strumentiresistenti.bear

import org.backuity.clist.{Cli, Command, args, arg, opt => cliOpt}
import System.err.{println => errln}

trait CommandRunner {
  def run: Unit = {}  
}

object OptsDefault {
  val defaultJdbcDriver = "com.cloudera.hive.jdbc4.HS2Driver" /* "org.apache.hive.jdbc.HiveDriver" */
  val defaultSrcUrl = "jdbc:hive2://localhost:10000/"  
  
  def urlCleaner(url: String) = url.replaceAll("/$", "")
}

object Opts {
  def parse(args: Array[String]) = {
    (Cli.parse(args).withCommands(Dump, Query)) getOrElse {
      errln("Unable to parse options")
      sys.exit(1)
    }
  }
}
