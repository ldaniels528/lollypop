package com.lollypop.language.models

import com.lollypop.language._
import com.lollypop.language.models.Operation.resolve
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Inferences.fastTypeResolve
import com.lollypop.runtime.datatypes.{CharType, Matrix}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.operators._
import com.lollypop.runtime.plastics.RuntimeClass.implicits._
import com.lollypop.runtime.plastics.Tuples.seqToArray
import lollypop.io.IOCost

import java.util.Date
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.language.postfixOps

/**
 * Represents an operator expression
 */
trait Operation extends Expression with RuntimeInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // handle binary operations (e.g. ("A" * 80))...
    this match {
      case op@BinaryOperation(a, b) =>
        val (sa, ca, va) = a.execute(scope)
        val (sb, cb, vb) = b.execute(sa)
        val vc = resolve(op, va, vb)(sb)
        (sb, ca ++ cb, vc)
      case x => this.dieIllegalType(x)
    }
  }

  def operator: String

}

/**
 * Math Operation
 */
object Operation {

  private def resolve(operation: Operation, aa: Any, bb: Any)(implicit scope: Scope): Any = {
    (operation, aa, bb) match {
      // null * x == null
      case (_, a, b) if a == null || b == null => null
      // arrays
      case (_: PlusPlus, a: Array[_], b: Array[_]) => seqToArray(a concat b)
      case (op, a: Array[_], b: Array[_]) => seqToArray((a zip b).map { case (x, y) => resolve(op, x, y) })
      case (op, a: Array[_], n: Number) => seqToArray(a.map(resolve(op, _, n)))
      case (op, n: Number, a: Array[_]) => seqToArray(a.map(resolve(op, n, _)))
      // booleans
      case (op, b: Boolean, n: Number) => resolveNumberToNumber(op, b.toInt, n)
      case (op, n: Number, b: Boolean) => resolveNumberToNumber(op, n, b.toInt)
      // chars
      case (op, c: Character, n: Number) => CharType.convert(resolveNumberToNumber(op, c.toInt, n))
      case (op, n: Number, c: Character) => resolveNumberToNumber(op, n, c.toInt)
      // dates & durations
      case (op, d: FiniteDuration, t: Date) => resolveAnyToAny(op, t, d)
      case (op, d: FiniteDuration, n: Number) => resolveAnyToAny(op, d, n.longValue().millis)
      // matrices
      case (op, n: Number, m: Matrix) => resolveAnyToAny(op, m, n)
      // numbers
      case (op, a: Number, b: Number) => resolveNumberToNumber(op, a, b)
      // strings
      case (op, x, s: String) => resolveAnyToAny(op, String.valueOf(x), s)
      // anything else
      case (op, x, y) => resolveAnyToAny(op, x, y)
    }
  }

  private def resolveNumberToNumber(op: Operation, aa: Number, bb: Number)(implicit scope: Scope): Any = {
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
      case _ => resolveAnyToAny(op, aa, bb)
    }
    fastTypeResolve(aa, bb).convert(result)
  }

  private def resolveAnyToAny(op: Operation, x: Any, y: Any)(implicit scope: Scope): AnyRef = {
    op match {
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
  }

}

