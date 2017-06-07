/*
   Bear -- A Hive metadata export tool -- TableDeps.scala
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

/**
 * The level of dependency of a symbol
 */
case class TableDependency(symbol: String, level: Int)

/**
 * The list of dependencies required to declare a symbol
 */
case class TableDependencies(symbol: String, deps: List[TableDependency]) {
  def levels = deps.map(_.level)
  
  def level = try {
    levels.max + 1
  } catch {
    case e: Exception => 1000
  }
  
  def resolved = !levels.contains(-1)
}

case class ViewCandidate(view: String, db: String, ddl: String)

object TableDeps {
  /*
   * This map contains the dependency stack
   */
  var deps: Map[Int, Set[String]] = Map()
  private var levels: Map[String, Int] = Map()
  private var viewCandidates: List[ViewCandidate] = List()
  var rejectedViews: List[ViewCandidate] = List()
  
  /*
   * Add a dependency to the stack
   */
  private def addDependency(symbol: String, level: Int): Unit = {
    /*
     * add to deps
     */
    val current = deps.getOrElse(level, Set())
    deps = deps ++ Map( (level, current + symbol) )
    
    /*
     * add to levels
     */
    levels += (symbol -> level)
  }
  
  /*
   * Add a table to the dependency stack. A Table level is by
   * definition 0.
   */
  def addTable(tbl: String) = addDependency(tbl, 0)
  
  /*
   * Extract the level of a symbol from the levels table
   */
  private def getSymbolLevel(symbol: String, db: String = "default") = {
    val withoutDb = symbol.replaceAll(s"${db}.", "")
    levels.getOrElse(symbol, levels.getOrElse(withoutDb, -1))
  }
  
  /*
   * Get the dependencies of a view
   */
  private def getViewDependencies(view: String, db: String, sources: List[String]) = {
    TableDependencies(view, sources.map { t => 
      TableDependency(t, getSymbolLevel(t, db)) 
    })
  }
  
  /*
   * Add a view to the dependency stack. The view DDL is evaluated
   * to extract all the source tables and views to properly compute
   * this view level of dependency
   */
  private def addView(view: String, db: String, ddl: String): Boolean = {
    /*
     * 1. extract the sources, doing some parsing
     */
    val sources: List[String] = 
       extractSources(ddl)
      .map(_.replaceAll(s"$db.", ""))
      .filter(!_.contains("."))
    
    /*
     * 2. compute the level of each source into a TableDependency
     */
    val td = getViewDependencies(view, db, sources)
    
    /*
     * 3. store this view level into the dependency map
     */
    if (!td.resolved) false
    else {
      addDependency(view, td.level)
      true
    }
  }
  
  /*
   * Parses a DDL and extract all its sources (both tables and views)
   */
  private def extractSources(ddl: String): List[String] = {
    /*
     * split the DDL into tokens
     */
    val tokens = ddl.replaceAll("\n", " ").split(" ").toList
    
    /*
     * pair each token with its next sibling
     */
    val sourcePairs = tokens.zip(tokens.tail)
    
    /*
     * filter token pairs where the left hand is a JOIN or a FROM
     * and the right hand does not start a subquery
     */
    val filteredPairs = sourcePairs.filter { case (l,r) =>
      (l.toLowerCase() == "join" || l.toLowerCase() == "from") &&
      r.length > 0 && r.charAt(0) != '('
    }
    
    /*
     * extract the raw sources and remove backticks
     */
    val rawSources = filteredPairs
      .map{ case (_,r) => r }
      .map{ _.replaceAll("`", "") }
    
    rawSources
  }
  
  /**
   * Add a view to the list of candidates to be resolved later
   */
  def addViewCandidate(view: String, db: String, ddl: String) = 
    viewCandidates = ViewCandidate(view, db, ddl) :: viewCandidates
  
  /**
   * Resolves the list of view candidates
   */
  def resolveViewCandidates: Unit = {
    def go(l: List[ViewCandidate], rej: List[ViewCandidate]): List[ViewCandidate] = l match {
      case Nil => rej
      case v :: vs => if (addView(v.view, v.db, v.ddl)) go(vs, rej) else go(vs, v :: rej)
    }
    
    def stackScan(l: List[ViewCandidate]): Unit = {
      if (!l.isEmpty) {
        val rej = go(l, Nil)
      
        if (rej.size == l.size) {
          Emitter.comment(s"""
            |--
            |-- Error resolving candidate views: ${rej.size} views remaining 
            |-- but none can be resolved for missing dependencies.
            |-- Unresolvable views are dumped at the end of this script.
            |-- """.stripMargin)
            
          // rej foreach { v => Emitter.comment(s"-- ${v.db}.${v.view}") }
          // Emitter.comment("-- ")
          // sys.exit(1)
          rejectedViews = rej
        } else {
          stackScan(rej)
        }
      }
    }
    
    stackScan(viewCandidates)
  }
  
  def clean: Unit = {
    deps = Map()
    levels = Map()
    viewCandidates = List()
    rejectedViews = List()
  }
}
