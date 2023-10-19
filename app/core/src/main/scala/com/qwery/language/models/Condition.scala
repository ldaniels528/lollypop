package com.qwery.language.models

import com.qwery.runtime.QweryNative
import com.qwery.runtime.datatypes.BooleanType
import com.qwery.runtime.instructions.conditions._
import com.qwery.runtime.instructions.expressions.ArrayLiteral

/**
 * Base class for all conditional expressions
 * @author lawrence.daniels@gmail.com
 */
trait Condition extends Expression with QweryNative {

  override def returnType: BooleanType = BooleanType

}

object Condition {

  /**
   * Condition Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class ConditionExtensions(val expr0: Expression) extends AnyVal {

    @inline def between(from: Expression, to: Expression): Condition = Between(expr0, from, to)

    @inline def betwixt(from: Expression, to: Expression): Condition = Betwixt(expr0, from, to)

    @inline def in(query: Instruction): Condition = IN(expr0, query)

    @inline def in(values: Expression*): Condition = IN(expr0, ArrayLiteral(values: _*))

    @inline def isNotNull: Condition = IsNotNull(expr0)

    @inline def isNull: Condition = IsNull(expr0)

  }

  /**
   * Rich Condition
   * @param cond0 the [[Condition math condition]]
   */
  final implicit class RichCondition(val cond0: Condition) extends AnyVal {

    @inline
    def negate: Condition = {
      cond0 match {
        case AND(a, b) => OR(a.negate, b.negate)
        case inequality: Inequality => inequality.invert
        case IsNotNull(a) => IsNull(a)
        case IsNull(a) => IsNotNull(a)
        case Not(condition) => condition
        case OR(a, b) => AND(a.negate, b.negate)
        case condition => Not(condition)
      }
    }
  }

}