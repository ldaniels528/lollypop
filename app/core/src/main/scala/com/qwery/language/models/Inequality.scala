package com.qwery.language.models

import com.qwery.die
import com.qwery.runtime.instructions.conditions._
import qwery.lang.Null

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

  /**
   * Inequality Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class InequalityExtensions(val expr0: Expression) extends AnyVal {

    @inline def is(expr1: Expression): Is = Is(expr0, expr1)

    @inline def isnt(expr1: Expression): Isnt = Isnt(expr0, expr1)

    @inline def ===(expr1: Expression): EQ = EQ(expr0, expr1)

    @inline def !==(expr1: Expression): NEQ = NEQ(expr0, expr1)

    @inline def >(expr1: Expression): GT = GT(expr0, expr1)

    @inline def >=(expr1: Expression): GTE = GTE(expr0, expr1)

    @inline def <(expr1: Expression): LT = LT(expr0, expr1)

    @inline def <=(expr1: Expression): LTE = LTE(expr0, expr1)

  }

  /**
   * Rich Inequality
   * @param inequality the [[Inequality math inequality]]
   */
  final implicit class RichInequality(val inequality: Inequality) extends AnyVal {

    @inline
    def invert: Inequality = inequality match {
      case EQ(a, b) => NEQ(a, b)
      case NEQ(a, b) => EQ(a, b)
      case Is(a, b) => Isnt(a, b)
      case Isnt(a, b) => Is(a, b)
      case LT(a, b) => GTE(a, b)
      case GT(a, b) => LTE(a, b)
      case LTE(a, b) => GT(a, b)
      case GTE(a, b) => LT(a, b)
      case x => die(s"Failed to negate inequality '$x'")
    }
  }

}