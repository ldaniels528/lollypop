package com.lollypop.runtime.conversions

import com.lollypop.language.dieIllegalType
import com.lollypop.runtime.devices.QMap

/**
 * Dictionary Conversion
 */
object DictionaryConversion extends Conversion {

  override def convert(value: Any): QMap[String, Any] = value match {
    case m: QMap[String, Any] => m
    case x => dieIllegalType(x)
  }

}
