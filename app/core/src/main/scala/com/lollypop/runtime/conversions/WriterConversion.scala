package com.lollypop.runtime.conversions

import com.lollypop.language.dieIllegalType

import java.io._

/**
 * Writer Conversion
 */
trait WriterConversion extends Conversion {

  override def convert(value: Any): Writer = {
    value match {
      case f: File => new FileWriter(f)
      case o: OutputStream => new OutputStreamWriter(o)
      case w: Writer => w
      case z => dieIllegalType(z)
    }
  }

}

/**
 * Writer Conversion Singleton
 */
object WriterConversion extends WriterConversion