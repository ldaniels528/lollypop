package com.lollypop.repl

import com.lollypop.util.ResourceHelper.AutoClose

import java.io.File
import scala.io.Source

/**
 * REPL Tools
 */
object REPLTools {

  def getResourceFile(rcFile: File): Option[String] = {
    if (rcFile.exists() && rcFile.isFile) {
      Console.println(s"Including '${rcFile.getAbsolutePath}' (${rcFile.length()} bytes)...'")
      Some(Source.fromFile(rcFile).use(_.mkString)).map(_.trim).flatMap(s => if (s.isEmpty) None else Some(s))
    } else None
  }

}
