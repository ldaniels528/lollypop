package com.lollypop.runtime.datatypes

/**
 * Represents a Fixed-Length Numeric Data Type
 * @param name           the name of the numeric data type
 * @param maxSizeInBytes the maximum length
 */
abstract class FixedLengthNumericDataType(name: String, maxSizeInBytes: Int) extends FixedLengthDataType(name, maxSizeInBytes) {

  override def isNumeric: Boolean = true

  override def isSigned: Boolean = true

}