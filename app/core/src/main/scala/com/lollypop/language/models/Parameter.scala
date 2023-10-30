package com.lollypop.language.models

import com.lollypop.die
import com.lollypop.util.StringRenderHelper

/**
 * Represents a logical parameter having a yet-to-be realized data type
 * @param name         the parameter name
 * @param `type`       the logical [[ColumnType parameter type]]
 * @param defaultValue the optional default value
 * @param isOutput     true, if the parameter is an output parameter
 */
case class Parameter(name: String,
                     `type`: ColumnType,
                     defaultValue: Option[Expression] = None,
                     isOutput: Boolean = false) extends ParameterLike {

  override def toString: String = StringRenderHelper.toProductString(this)

}

/**
 * Parameter Companion
 */
object Parameter {

  /**
   * Constructs a new parameter from the given descriptor
   * @param descriptor the parameter descriptor (e.g. "symbol string")
   * @return a new [[Parameter]]
   */
  def apply(descriptor: String): Parameter = descriptor.split("[ ]").toList match {
    case name :: _type :: Nil =>
      new Parameter(name, `type` = ColumnType(_type))
    case unknown =>
      die(s"Invalid parameter descriptor '$unknown'")
  }

}