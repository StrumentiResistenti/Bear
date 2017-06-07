/*
   Bear -- A Hive metadata export tool -- Emitter.scala
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

import System.err.{println => errln}
import org.backuity.clist.Command

object Emitter {
  type EmitterFunc = (String, Boolean) => Unit
  
  def nullEmitterFin() = {}
  
  private var buffer: List[String] = _
  private var outputFile: Option[java.io.PrintWriter] = _
  private var emitter: EmitterFunc = _

  /**
   * initialize the emitter object
   */
  def init: Unit = {    
    outputFile = if (Dump.output.length == 0) None else {
      Some(new java.io.PrintWriter(new java.io.File(Dump.output))) 
    }
    
    emitter = if (Dump.dstUrl != "") emitToDestination
              else if (!outputFile.isEmpty) emitToOutput
              else emitToStdout
    reset
  }
  
  /**
   * finalize the emitter object
   */
  def fin: Unit = {
    if (!outputFile.isEmpty) outputFile.get.close()
  }
  
  /**
   * reset the emitter object
   */
  def reset: Unit = {
    buffer = List()
  }
  
  /**
   * just print each statement on STDOUT
   * 
   * @param s the query to emit
   * @param comment true if s is a comment
   */
  def emitToStdout(s: String, comment: Boolean) = println(if (comment) s else s"$s;")
  
  /**
   * append each statement to the output filename provided on command line
   * 
   * @param s the query to emit
   * @param comment true if s is a comment
   */
  def emitToOutput(s: String, comment: Boolean) = outputFile.get.println(if (comment) s else s"$s;")
  
  /**
   * append SQL statement to internal buffer which should be flushed with emitBuffer()
   * 
   * @param s the query to emit
   * @param comment true if s is a comment
   */
  def emitToDestination(s: String, comment: Boolean) = if (!comment) buffer = s :: buffer
  
  /*
   * shortcuts to emit comments and statements
   */
  def emit(s: String) = emitter(s, false)
  def comment(s: String) = emitter(s, true)

  /**
   * flush the SQL buffer that holds statement to be reproduced on destination DB
   */
  def emitBuffer: Unit = {
  	if (!buffer.isEmpty) {
      val dst = new Bear(Dump.dstDriver, Dump.dstPureUrl, Dump.dstUser, Dump.dstPass)
      
      def emitBufferStatement(s: String): Unit = try {
        dst.exec(s)
      } catch {
        case e: Exception => {
          errln(s"Error running query on destination: ${e.getMessage}")
          errln(s"[$s]")
          errln(e.getStackTraceString)
        }      
      }
    
      buffer.reverse.foreach(emitBufferStatement)
	  }
  }
}
