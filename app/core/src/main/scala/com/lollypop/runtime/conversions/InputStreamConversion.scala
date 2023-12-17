package com.lollypop.runtime.conversions

import com.lollypop.runtime.LollypopVM.rootScope
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.queryables.TableRendering

import java.io._
import java.net.URL
import scala.annotation.tailrec

/**
 * Input Stream Conversion
 */
trait InputStreamConversion extends Conversion {

  @tailrec
  final override def convert(value: Any): InputStream = value match {
    case a: Array[Byte] => new ByteArrayInputStream(a)
    case a: Array[Char] => convert(String.valueOf(a))
    case b: java.sql.Blob => b.getBinaryStream
    case c: Char => convert(String.valueOf(c))
    case c: java.sql.Clob => c.getAsciiStream
    case f: File => new FileInputStream(f)
    case i: InputStream => i
    case s: String => convert(s.getBytes)
    case x: java.sql.SQLXML => x.getBinaryStream
    case t: TableRendering => convert(t.toTable(rootScope).tabulate().mkString("\n"))
    case t: Throwable =>
      val out = new ByteArrayOutputStream(256)
      new PrintStream(out).use(t.printStackTrace)
      convert(out.toByteArray)
    case u: URL => u.openStream()
    case z => convert(String.valueOf(z))
  }

}

/**
 * Input Stream Conversion Singleton
 */
object InputStreamConversion extends InputStreamConversion