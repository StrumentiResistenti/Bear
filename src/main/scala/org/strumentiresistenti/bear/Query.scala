/*
   Bear -- A Hive metadata export tool -- Query.scala
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

