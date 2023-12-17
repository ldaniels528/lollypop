package com.lollypop.language

import com.lollypop.language.models._
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.expressions._

import scala.language.{implicitConversions, postfixOps, reflectiveCalls}

/**
 * language implicits
 */
package object implicits {

  /**
   * Boolean to Expression conversion
   * @param value the given Boolean value
   * @return the equivalent [[BooleanLiteral]]
   */
  final implicit def boolean2Expr(value: Boolean): BooleanLiteral = BooleanLiteral(value)

  /**
   * Byte to Expression conversion
   * @param value the given Byte value
   * @return the equivalent [[Expression]]
   */
  final implicit def byte2Expr(value: Byte): Expression = Literal(value)

  /**
   * Char to Expression conversion
   * @param value the given Char value
   * @return the equivalent [[Expression]]
   */
  final implicit def char2Expr(value: Char): Expression = Literal(value)

  /**
   * Date to Expression conversion
   * @param value the given Date value
   * @return the equivalent [[Expression]]
   */
  final implicit def date2Expr(value: java.util.Date): Expression = Literal(value)

  /**
   * Double to Expression conversion
   * @param value the given Double value
   * @return the equivalent [[Expression]]
   */
  final implicit def double2Expr(value: Double): Expression = Literal(value)

  /**
   * Float to Expression conversion
   * @param value the given Float value
   * @return the equivalent [[Expression]]
   */
  final implicit def float2Expr(value: Float): Expression = Literal(value)

  /**
   * Integer to Expression conversion
   * @param value the given Integer value
   * @return the equivalent [[Expression]]
   */
  final implicit def int2Expr(value: Int): Expression = Literal(value)

  /**
   * Long to Expression conversion
   * @param value the given Long value
   * @return the equivalent [[Expression]]
   */
  final implicit def long2Expr(value: Long): Expression = Literal(value)

  /**
   * Map to Expression conversion
   * @param values the given map of values
   * @return the equivalent [[Dictionary]]
   */
  final implicit def map2Expr(values: Map[String, Expression]): Dictionary = Dictionary(values.toSeq: _*)

  /**
   * Short Integer to Expression conversion
   * @param value the given Short integer value
   * @return the equivalent [[Expression]]
   */
  final implicit def short2Expr(value: Short): Expression = Literal(value)

  /**
   * String to Expression conversion
   * @param value the given String value
   * @return the equivalent [[Expression]]
   */
  final implicit def string2Expr(value: String): Expression = Literal(value)

  /**
   * Symbol to Field conversion
   * @param symbol the given field name
   * @return the equivalent [[FieldRef]]
   */
  final implicit def symbolToField(symbol: Symbol): FieldRef = FieldRef(symbol.name)

  /**
   * Symbol to Ordered Column conversion
   * @param symbol the given column name
   * @return the equivalent [[OrderColumn]]
   */
  final implicit def symbolToOrderColumn(symbol: Symbol): OrderColumn = OrderColumn(symbol.name, isAscending = true)

}
