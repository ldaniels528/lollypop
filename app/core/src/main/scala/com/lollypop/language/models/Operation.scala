package com.lollypop.language.models

import com.lollypop.language._
import com.lollypop.language.models.Operation.evaluateAny
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Inferences.fastTypeResolve
import com.lollypop.runtime.datatypes.{CharType, Matrix}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.operators._
import com.lollypop.runtime.plastics.RuntimeClass.implicits._
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
        val vc = evaluateAny(op, va, vb)(sb)
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

  private def evaluateAny(op: Operation, aa: Any, bb: Any)(implicit scope: Scope): Any = {
    (aa, bb) match {
      // null * x == null
      case (a, b) if a == null || b == null => null
      // arrays
      case (a: Array[_], b: Array[_]) => (a zip b).map { case (x, y) => evaluateAny(op, x, y) }
      case (a: Array[_], x) => a.map(evaluateAny(op, _, x))
      // booleans
      case (b: Boolean, n: Number) => evaluateNumber(op, b.toInt, n)
      case (n: Number, b: Boolean) => evaluateNumber(op, n, b.toInt)
      // chars
      case (c: Character, n: Number) => CharType.convert(evaluateNumber(op, c.toInt, n))
      case (n: Number, c: Character) => evaluateNumber(op, n, c.toInt)
      // dates & durations
      case (d: FiniteDuration, t: Date) => evaluateVM(op, t, d)
      case (d: FiniteDuration, n: Number) => evaluateVM(op, d, n.longValue().millis)
      // matrices
      case (n: Number, m: Matrix) => evaluateVM(op, m, n)
      // numbers
      case (a: Number, b: Number) => evaluateNumber(op, a, b)
      // strings
      case (x, s: String) => evaluateVM(op, String.valueOf(x), s)
      // anything else
      case (x, y) => evaluateVM(op, x, y)
    }
  }

  private def evaluateNumber(op: Operation, aa: Number, bb: Number)(implicit scope: Scope): Any = {
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
      case _ => evaluateVM(op, aa, bb)
    }
    fastTypeResolve(aa, bb).convert(result)
  }

  private def evaluateVM(op: Operation, x: Any, y: Any)(implicit scope: Scope): AnyRef = {
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

