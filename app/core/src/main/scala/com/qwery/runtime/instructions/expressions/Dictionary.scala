package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.{Expression, Literal}
import com.qwery.runtime.datatypes.{AnyType, DataType}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.{QweryVM, Scope}

import scala.collection.mutable

/**
 * Represents a Dictionary
 * @param value the dictionary entries
 * @example {{{
 * val response = { 'message' : 'Hello World' }
 * response.message
 * }}}
 * @example {{{
 * val response = { 'message' : 'Hello World' }
 * response.message = 'Hallo Monde'
 * }}}
 */
case class Dictionary(value: List[(String, Expression)]) extends Literal with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): mutable.LinkedHashMap[String, Any] = {
    mutable.LinkedHashMap(value.map { case (name, expr) => name -> QweryVM.execute(scope, expr)._3 }: _*)
  }

  override def returnType: DataType = AnyType(className = classOf[mutable.LinkedHashMap.type].getName)

  override def toSQL: String = {
    value.map {
      case (k, v) if k.nonEmpty && k.forall(c => c == '_' || c.isLetterOrDigit) => s"$k: ${v.toSQL}"
      case (k, v) => s"\"$k\": ${v.toSQL}"
    } mkString("{ ", ", ", " }")
  }

}

/**
 * Dictionary Companion
 */
object Dictionary {

  /**
   * Creates a new dictionary
   * @param entries the dictionary entries
   * @return a new [[Dictionary]]
   */
  def apply(entries: (String, Expression)*): Dictionary = new Dictionary(entries.toList)

  /**
   * Creates a new dictionary
   * @param entries the dictionary entries
   * @return a new [[Dictionary]]
   */
  def apply(entries: QMap[String, Expression]): Dictionary = new Dictionary(entries.toList)

}