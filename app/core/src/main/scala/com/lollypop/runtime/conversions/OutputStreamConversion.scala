package com.lollypop.runtime.conversions

import com.lollypop.language.dieIllegalType

import java.io._

/**
 * Output Stream Conversion
 */
trait OutputStreamConversion extends Conversion {

  override def convert(value: Any): OutputStream = value match {
    case b: java.sql.Blob => b.setBinaryStream(0)
    case c: java.sql.Clob => c.setAsciiStream(0)
    case f: File => new FileOutputStream(f)
    case o: OutputStream => o
    case x: java.sql.SQLXML => x.setBinaryStream()
    case z => dieIllegalType(z)
  }

}

/**
 * Output Stream Conversion Singleton
 */
object OutputStreamConversion extends OutputStreamConversion