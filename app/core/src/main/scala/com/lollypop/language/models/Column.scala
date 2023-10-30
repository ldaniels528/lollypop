package com.lollypop.language.models

import com.lollypop.die
import com.lollypop.util.StringRenderHelper

/**
 * Represents a logical column having a yet-to-be realized content type
 * @param name         the column name
 * @param `type`       the logical [[ColumnType column type]]
 * @param defaultValue the optional default value
 * @param isRowID      indicates whether the column returns the row ID
 */
case class Column(name: String, `type`: ColumnType, defaultValue: Option[Expression] = None, isRowID: Boolean = false)
  extends ParameterLike {

  override def isOutput: Boolean = false

  override def toString: String = StringRenderHelper.toProductString(this)

}

/**
 * Column Companion
 */
object Column {

  /**
   * Constructs a new column from the given descriptor
   * @param descriptor the column descriptor (e.g. "symbol string")
   * @return a new [[Column]]
   */
  def apply(descriptor: String): Column = {
    (if (descriptor contains ":") descriptor.split(":").map(_.trim) else descriptor.split(" ")).toList match {
      case name :: _type :: Nil =>
        new Column(name, `type` = ColumnType(_type))
      case unknown =>
        die(s"Invalid column descriptor '$unknown'")
    }
  }

}
