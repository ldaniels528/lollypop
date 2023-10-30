package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion

/**
 * Represents the default base class for data types
 * @param name the name of the data type
 */
abstract class AbstractDataType(val name: String) extends DataType {

  protected def convertCurrency(name: String, value: Any): Double = value match {
    case n: Number => n.doubleValue()
    case s: String =>
      val text = s.toUpperCase.filterNot(c => c == '$' || c == ',') // e.g., $54M or $11,000.00
      text match {
        case v if v.endsWith("K") => v.dropRight(1).toDouble * 1e+3
        case v if v.endsWith("M") => v.dropRight(1).toDouble * 1e+6
        case v if v.endsWith("B") => v.dropRight(1).toDouble * 1e+9
        case v if v.endsWith("T") => v.dropRight(1).toDouble * 1e+12
        case v if v.endsWith("Q") => v.dropRight(1).toDouble * 1e+15
        case v => v.toDouble
      }
    case x => dieUnsupportedConversion(x, name)
  }

}