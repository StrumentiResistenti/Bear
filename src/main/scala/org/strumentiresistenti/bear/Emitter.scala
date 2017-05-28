package org.strumentiresistenti.bear

import System.err.{println => errln}

object Emitter {
  type EmitterFunc = (String, Boolean) => Unit
  
  /**
   * Emit buffer and output channel
   */
  var buffer: List[String] = _
  var outputFile: Option[java.io.PrintWriter] = _
  var emitter: EmitterFunc = _
  var opts: Opts = _

  /*
   * initialize the Emitter object
   */
  def init(opts: Opts): Unit = {
    this.opts = opts
    
    outputFile = if (opts.output.length == 0) None else {
      Some(new java.io.PrintWriter(new java.io.File(opts.output))) 
    }
    
    emitter = if (opts.dstUrl != "") emitToDestination
              else if (!outputFile.isEmpty) emitToOutput
              else emitToStdout

    reset
  }
  
  def reset: Unit = {
    buffer = List()
  }
  
  /*
   * just print each statement on STDOUT
   */
  def emitToStdout(s: String, comment: Boolean) = println(s)
  
  /*
   * append each statement to the output filename provided on command line
   */
  def emitToOutput(s: String, comment: Boolean) = outputFile.get.println(s)
  
  /*
   * append SQL statement to internal buffer which should be flushed with emitBuffer()
   */
  def emitToDestination(s: String, comment: Boolean) = if (!comment) buffer = s :: buffer
  
  /*
   * shortcuts to emit comments and statements
   */
  def emit(s: String) = emitter(s, false)
  def comment(s: String) = emitter(s, true)

  /*
   * flush the SQL buffer that holds statement to be reproduced on destination DB
   */
  def emitBuffer: Unit = {
    val dst = new Bear(opts.dstDriver, opts.dstPureUrl, opts.dstUser, opts.dstPass)
    
    def emitBufferStatement(s: String): Unit = try {
      dst.exec(s)
    } catch {
      case e: Exception => {
        errln(s"Error running query on destination: ${e.getMessage}")
        errln(s"[$s]")
        errln(e.getStackTrace)
      }      
    }
  
    buffer.reverse.foreach(emitBufferStatement)
  }
}