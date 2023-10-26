package com.qwery.language.models

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.plastics.RuntimeClass.implicits.RuntimeClassInstanceSugar
import com.qwery.runtime.datatypes.Inferences.fastTypeResolve
import com.qwery.runtime.datatypes.Matrix
import com.qwery.runtime.instructions.RuntimeInstruction
import com.qwery.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.qwery.runtime.instructions.operators._
import com.qwery.runtime.{Boolean2Int, QweryVM, Scope}
import qwery.io.IOCost

import java.util.Date
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.language.postfixOps

/**
 * Represents an operator expression
 */
sealed trait Operation extends Expression with RuntimeInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, evaluateOperator(this)(scope))
  }

  def operator: String

  private def evaluateOperator(op: Operation)(implicit scope: Scope): Any = {

    def evaluateNumber(aa: Number, bb: Number): Any = {
      val result = op match {
        case _: Amp => aa.longValue() & bb.longValue()
        case _: Bar => aa.longValue() | bb.longValue()
        case _: Div =>
          val bbb = bb.doubleValue()
          if (bbb == 0.0) op.dieDivisionByZero(op.toSQL) else aa.doubleValue() / bbb
        case _: GreaterGreater => aa.longValue() >> bb.longValue()
        case _: GreaterGreaterGreater => aa.longValue() >>> bb.longValue()
        case _: LessLess => aa.longValue() << bb.longValue()
        case _: LessLessLess => aa.longValue() << bb.longValue()
        case _: Minus => aa.doubleValue() - bb.doubleValue()
        case _: Percent => aa.longValue() % bb.longValue()
        case _: Plus => aa.doubleValue() + bb.doubleValue()
        case _: Times => aa.doubleValue() * bb.doubleValue()
        case _: TimesTimes => Math.pow(aa.doubleValue(), bb.doubleValue())
        case _: Up => aa.longValue() ^ bb.longValue()
        case _ => evaluateAny(aa, bb)
      }
      fastTypeResolve(aa, bb).convert(result)
    }

    def evaluateAny(x: Any, y: Any): AnyRef = op match {
      case _: Amp => x.invokeMethod("$amp", Seq(y.v))
      case _: AmpAmp => x.invokeMethod("$amp$amp", Seq(y.v))
      case _: Bar => x.invokeMethod("$bar", Seq(y.v))
      case _: BarBar => x.invokeMethod("$bar$bar", Seq(y.v))
      case _: ColonColon => x.invokeMethod("$colon$colon", Seq(y.v))
      case _: ColonColonColon => x.invokeMethod("$colon$colon$colon", Seq(y.v))
      case _: Div => x.invokeMethod("$div", Seq(y.v))
      case _: GreaterGreater => x.invokeMethod("$greater$greater", Seq(y.v))
      case _: LessLess => x.invokeMethod("$less$less", Seq(y.v))
      case _: Minus => x.invokeMethod("$minus", Seq(y.v))
      case _: MinusMinus => x.invokeMethod("$minus$minus", Seq(y.v))
      case _: Percent => x.invokeMethod("$percent", Seq(y.v))
      case _: PercentPercent => x.invokeMethod("$percent$percent", Seq(y.v))
      case _: Plus => x.invokeMethod("$plus", Seq(y.v))
      case _: PlusPlus => x.invokeMethod("$plus$plus", Seq(y.v))
      //case _: Tilde => x.invokeMethod("$tilde", Seq(y.v))
      case _: Times => x.invokeMethod("$times", Seq(y.v))
      case _: TimesTimes => x.invokeMethod("$times$times", Seq(y.v))
      case _: Up => x.invokeMethod("$up", Seq(y.v))
      case _ => op.die(s"Cannot execute ${op.toSQL}")
    }

    op match {
      case BinaryOperation(a, b) =>
        val (aa, bb) = (QweryVM.execute(scope, a)._3, QweryVM.execute(scope, b)._3)
        (aa, bb) match {
          // dates & durations
          case (d: FiniteDuration, t: Date) => evaluateAny(t, d)
          case (d: FiniteDuration, n: Number) => evaluateAny(d, n.longValue().millis)
          // number +-/* number
          case (aa: Number, bb: Number) => evaluateNumber(aa, bb)
          case (b: Boolean, n: Number) => evaluateNumber(b.toInt, n)
          case (n: Number, b: Boolean) => evaluateNumber(n, b.toInt)
          case (c: Character, n: Number) => evaluateNumber(c.toInt, n)
          case (n: Number, c: Character) => evaluateNumber(n, c.toInt)
          case (n: Number, m: Matrix) => evaluateAny(m, n)
          // string + anything
          case (x, s: String) => evaluateAny(String.valueOf(x), s)
          case (x, y) => evaluateAny(x, y)
        }
      case x => op.dieIllegalType(x)
    }
  }

}

/**
 * Represents an unary operator expression
 */
trait UnaryOperation extends Operation {
  def a: Expression

  override def toSQL: String = a match {
    case i: IdentifierRef => s"$operator${i.toSQL}"
    case e => s"$operator(${e.toSQL})"
  }
}

object UnaryOperation {
  def unapply(o: UnaryOperation): Option[Expression] = Some(o.a)
}

/**
 * Represents a binary operator expression
 */
trait BinaryOperation extends Operation {
  def a: Expression

  def b: Expression

  override def toSQL: String = s"${a.wrapSQL} $operator ${b.wrapSQL}"
}

object BinaryOperation {
  def unapply(o: BinaryOperation): Option[(Expression, Expression)] = Some((o.a, o.b))
}

/**
 * Math Operation
 */
object Operation {

  /**
   * Operation Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class RichOperation(val expr0: Expression) extends AnyVal {

    @inline def +(expr1: Expression): Plus = Plus(expr0, expr1)

    @inline def +=(expr1: Expression): ComputeAndSet = Plus(expr0, expr1).doAndSet

    @inline def ++(expr1: Expression): PlusPlus = PlusPlus(expr0, expr1)

    @inline def &(expr1: Expression): Amp = Amp(expr0, expr1)

    @inline def &=(expr1: Expression): ComputeAndSet = Amp(expr0, expr1).doAndSet

    @inline def &&(expr1: Expression): AmpAmp = AmpAmp(expr0, expr1)

    @inline def &&=(expr1: Expression): ComputeAndSet = AmpAmp(expr0, expr1).doAndSet

    @inline def |(expr1: Expression): Bar = Bar(expr0, expr1)

    @inline def |=(expr1: Expression): ComputeAndSet = Bar(expr0, expr1).doAndSet

    @inline def ||(expr1: Expression): BarBar = BarBar(expr0, expr1)

    @inline def ||=(expr1: Expression): ComputeAndSet = BarBar(expr0, expr1).doAndSet

    @inline def ::(expr1: Expression): ColonColon = ColonColon(expr1, expr0)

    @inline def ::=(expr1: Expression): ComputeAndSet = ColonColon(expr0, expr1).doAndSet

    @inline def :::(expr1: Expression): ColonColonColon = ColonColonColon(expr1, expr0)

    @inline def :::=(expr1: Expression): ComputeAndSet = ColonColonColon(expr0, expr1).doAndSet

    @inline def ^(expr1: Expression): Up = Up(expr0, expr1)

    @inline def ^=(expr1: Expression): ComputeAndSet = Up(expr0, expr1).doAndSet

    @inline def /(expr1: Expression): Div = Div(expr0, expr1)

    @inline def /=(expr1: Expression): ComputeAndSet = Div(expr0, expr1).doAndSet

    @inline def >>(expr1: Expression): GreaterGreater = GreaterGreater(expr0, expr1)

    @inline def >>=(expr1: Expression): ComputeAndSet = GreaterGreater(expr0, expr1).doAndSet

    @inline def >>>(expr1: Expression): GreaterGreaterGreater = GreaterGreaterGreater(expr0, expr1)

    @inline def >>>=(expr1: Expression): ComputeAndSet = GreaterGreaterGreater(expr0, expr1).doAndSet

    @inline def <<(expr1: Expression): LessLess = LessLess(expr0, expr1)

    @inline def <<=(expr1: Expression): ComputeAndSet = LessLess(expr0, expr1).doAndSet

    @inline def <<<(expr1: Expression): LessLessLess = LessLessLess(expr0, expr1)

    @inline def <<<=(expr1: Expression): ComputeAndSet = LessLessLess(expr0, expr1).doAndSet

    @inline def %(expr1: Expression): Percent = Percent(expr0, expr1)

    @inline def %%(expr1: Expression): PercentPercent = PercentPercent(expr0, expr1)

    @inline def %=(expr1: Expression): ComputeAndSet = Percent(expr0, expr1).doAndSet

    @inline def *(expr1: Expression): Times = Times(expr0, expr1)

    @inline def *=(expr1: Expression): ComputeAndSet = Times(expr0, expr1).doAndSet

    @inline def **(expr1: Expression): TimesTimes = TimesTimes(expr0, expr1)

    @inline def **=(expr1: Expression): ComputeAndSet = TimesTimes(expr0, expr1).doAndSet

    @inline def -(expr1: Expression): Minus = Minus(expr0, expr1)

    @inline def --(expr1: Expression): MinusMinus = MinusMinus(expr0, expr1)

    @inline def -=(expr1: Expression): ComputeAndSet = Minus(expr0, expr1).doAndSet

  }

}

