package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.dieUnsupportedEntity
import com.lollypop.language.models._
import com.lollypop.runtime.datatypes.BooleanType
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * Runtime Condition
 */
trait RuntimeCondition extends Condition with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Boolean = isTrue

  def isTrue(implicit scope: Scope): Boolean

  def isFalse(implicit scope: Scope): Boolean = !isTrue

}

/**
 * Run-time Condition Companion
 */
object RuntimeCondition {

  def isTrue(expression: Expression)(implicit scope: Scope): Boolean = expression match {
    case cond: RuntimeCondition => cond.isTrue
    case expr: RuntimeExpression => BooleanType.convert(expr.evaluate())
    case queryable: RuntimeQueryable => BooleanType.convert(LollypopVM.search(scope, queryable)._3)
    case unknown => dieUnsupportedEntity(unknown, entityName = "condition")
  }

  def isFalse(expression: Expression)(implicit scope: Scope): Boolean = !isTrue(expression)

  /**
   * Rich Condition
   * @param condition the host [[Condition condition]]
   */
  final implicit class RichConditionAtRuntime(val condition: Condition) extends AnyVal {

    def collect[A](f: PartialFunction[Condition, A]): List[A] = {
      @inline
      def resolve(condition: Condition): List[A] = {
        condition match {
          case AND(a, b) => resolve(a) ::: resolve(b)
          case Not(c) => resolve(c)
          case OR(a, b) => resolve(a) ::: resolve(b)
          case c if f.isDefinedAt(c) => List(f(c))
          case _ => Nil
        }
      }

      resolve(condition)
    }

    def rewrite(f: PartialFunction[Condition, Option[Condition]]): Option[Condition] = {
      @inline
      def filter(condition: Condition): Option[Condition] = {
        condition match {
          case AND(a, b) => reduce(a, b, AND.apply)(filter)
          case Not(c) => filter(c).map(Not.apply)
          case OR(a, b) => reduce(a, b, OR.apply)(filter)
          case c if f.isDefinedAt(c) => f(c)
          case c => Some(c)
        }
      }

      filter(condition)
    }

    def select(f: PartialFunction[Condition, Condition]): Option[Condition] = {
      @inline
      def filter(condition: Condition): Option[Condition] = {
        condition match {
          case AND(a, b) => reduce(a, b, AND.apply)(filter)
          case Not(c) => filter(c).map(Not.apply)
          case OR(a, b) => reduce(a, b, OR.apply)(filter)
          case c if f.isDefinedAt(c) => Option(f(c))
          case _ => None
        }
      }

      filter(condition)
    }

    def transform(f: Expression => Expression): Condition = {
      condition match {
        case AND(a, b) => AND(a.transform(f), b.transform(f))
        case Between(expr, a, b) => Between(f(expr), f(a), f(b))
        case Betwixt(expr, a, b) => Betwixt(f(expr), f(a), f(b))
        case EQ(a, b) => EQ(f(a), f(b))
        case GT(a, b) => GT(f(a), f(b))
        case GTE(a, b) => GTE(f(a), f(b))
        case Is(a, b) => Is(f(a), f(b))
        case Isnt(a, b) => Isnt(f(a), f(b))
        case Matches(a, b) => Matches(f(a), f(b))
        case LT(a, b) => LT(f(a), f(b))
        case LTE(a, b) => LTE(f(a), f(b))
        case IsNotNull(a) => IsNotNull(f(a))
        case IsNull(a) => IsNull(f(a))
        case NEQ(a, b) => NEQ(f(a), f(b))
        case Not(c) => Not(c.transform(f))
        case OR(a, b) => OR(a.transform(f), b.transform(f))
        case WhereIn(expr, c) => WhereIn(f(expr), c.transform(f))
        case cond => cond
      }
    }

    @inline
    private def reduce(a: Condition, b: Condition, f: (Condition, Condition) => Condition)(filter: Condition => Option[Condition]): Option[Condition] = {
      (filter(a), filter(b)) match {
        case (Some(aa), Some(bb)) => Option(f(aa, bb))
        case (v@Some(_), None) => v
        case (None, v@Some(_)) => v
        case (None, None) => None
      }
    }

  }

}