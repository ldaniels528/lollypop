package com.lollypop.runtime.datatypes

import com.lollypop.language.models.ColumnType

/**
 * Represents a Fixed-Length Data Type
 * @param name           the name of the data type
 * @param maxSizeInBytes the maximum length
 */
abstract class FixedLengthDataType(name: String, val maxSizeInBytes: Int) extends AbstractDataType(name) {

  override def isFixedLength: Boolean = true

  override def toColumnType: ColumnType = ColumnType(name = name)

}