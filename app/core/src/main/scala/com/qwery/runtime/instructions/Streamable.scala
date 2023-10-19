package com.qwery.runtime.instructions

import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.ResourceHelper.AutoClose

import java.io._
import scala.annotation.tailrec

object Streamable {

  def getInputStream(expr: Expression)(implicit scope: Scope): InputStream = {
    @tailrec
    def recurse(value: Any): InputStream = value match {
      case a: Array[Byte] => new ByteArrayInputStream(a)
      case a: Array[Char] => recurse(String.valueOf(a))
      case b: java.sql.Blob => b.getBinaryStream
      case c: java.sql.Clob => c.getAsciiStream
      case f: File => new FileInputStream(f)
      case i: InputStream => i
      case s: String => recurse(s.getBytes)
      case x: java.sql.SQLXML => x.getBinaryStream
      case t: TableRendering => recurse(t.toTable.tabulate().mkString("\n"))
      case t: Throwable =>
        val out = new ByteArrayOutputStream(256)
        new PrintStream(out).use(t.printStackTrace)
        recurse(out.toByteArray)
      case z => recurse(z.toString)
    }

    val (_, cost0, result0) = QweryVM.execute(scope, expr)
    recurse(result0)
  }

  def getOutputStream(expr: Expression)(implicit scope: Scope): OutputStream = {
    val (_, cost0, result0) = QweryVM.execute(scope, expr)
    result0 match {
      case b: java.sql.Blob => b.setBinaryStream(0)
      case c: java.sql.Clob => c.setAsciiStream(0)
      case f: File => new FileOutputStream(f)
      case o: OutputStream => o
      case s: String =>
        val baos = new ByteArrayOutputStream(s.length)
        baos.write(s.getBytes)
        baos
      case x: java.sql.SQLXML => x.setBinaryStream()
      case z => expr.dieIllegalType(z)
    }
  }

  def getReader(expr: Expression)(implicit scope: Scope): Reader = {
    val (_, cost0, result0) = QweryVM.execute(scope, expr)
    result0 match {
      case f: File => new FileReader(f)
      case r: Reader => r
      case s: String => new StringReader(s)
      case z => expr.dieIllegalType(z)
    }
  }

  def getWriter(expr: Expression)(implicit scope: Scope): Writer = {
    val (_, cost0, result0) = QweryVM.execute(scope, expr)
    result0 match {
      case f: File => new FileWriter(f)
      case s: String =>
        val w = new StringWriter(s.length)
        w.write(s)
        w
      case w: Writer => w
      case z => expr.dieIllegalType(z)
    }
  }

}
