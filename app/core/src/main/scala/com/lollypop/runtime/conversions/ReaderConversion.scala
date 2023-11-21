package com.lollypop.runtime.conversions

import com.lollypop.language.dieIllegalType

import java.io.{File, FileReader, InputStream, InputStreamReader, Reader, StringReader}

/**
 * Reader Conversion
 */
trait ReaderConversion extends Conversion {

  override def convert(value: Any): Reader = {
    value match {
      case f: File => new FileReader(f)
      case i: InputStream => new InputStreamReader(i)
      case r: Reader => r
      case s: String => new StringReader(s)
      case z => dieIllegalType(z)
    }
  }

}

/**
 * Reader Conversion Singleton
 */
object ReaderConversion extends ReaderConversion