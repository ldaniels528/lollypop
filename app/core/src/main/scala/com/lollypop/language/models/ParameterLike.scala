package com.lollypop.language.models

/**
 * Represent a parameter-like entity; e.g. function parameter or table column
 */
trait ParameterLike extends Instruction {

  /**
   * @return the column name
   */
  def name: String

  /**
   * @return the logical [[ColumnType parameter type]]
   */
  def `type`: ColumnType

  /**
   * @return the optional default value
   */
  def defaultValue: Option[Expression]

  /**
   * @return true, if the parameter is an output parameter
   */
  def isOutput: Boolean

  override def toSQL: String = ((if (isOutput) List("-->") else Nil) ::: List(name + ":", `type`.toSQL) :::
    defaultValue.toList.flatMap(v => List("=", v.toSQL))).mkString(" ")

}

object ParameterLike {

  final implicit class RichParameterLike(val parameterLike: ParameterLike) extends AnyVal {
    @inline
    def toColumn: Column = parameterLike match {
      case c: Column => c
      case p => Column(name = p.name, `type` = p.`type`, defaultValue = p.defaultValue)
    }

    @inline
    def toParameter: Parameter = parameterLike match {
      case p: Parameter => p
      case p => Parameter(name = p.name, `type` = p.`type`, defaultValue = p.defaultValue)
    }
  }

}