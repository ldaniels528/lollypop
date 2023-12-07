package com.lollypop.language.models

import com.lollypop.runtime.instructions.conditions._
import lollypop.lang.Null

/**
 * Represents an Inequality expression
 */
trait Inequality extends Condition {

  def a: Expression

  def b: Expression

  def operator: String

  override def toSQL: String = s"${a.wrapSQL} $operator ${b.wrapSQL}"

}

/**
 * Inequality Companion
 */
object Inequality {

  def toInequalities(condition: Condition): List[Inequality] = {
    def recurse(condition: Condition): List[Inequality] = {
      condition match {
        case AND(a, b) => recurse(a) ::: recurse(b)
        case Between(expression, from, to) => List(GTE(expression, from), LTE(expression, to))
        case Betwixt(expression, from, to) => List(GTE(expression, from), LT(expression, to))
        case inequality: Inequality => inequality :: Nil
        case IsNotNull(expression) => NEQ(expression, Null()) :: Nil
        case IsNull(expression) => EQ(expression, Null()) :: Nil
        case Not(condition) => recurse(condition).map(_.invert)
        case OR(a, b) => recurse(a) ::: recurse(b)
        case _ => Nil
      }
    }

    // recursively processes all conditions
    recurse(condition)
  }

  def unapply(inequality: Inequality): Option[(Expression, Expression, String)] = Some((inequality.a, inequality.b, inequality.operator))

}