package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.{Expression, UnaryOperation}
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * Negates an expression (e.g. "-(7 + 9)")
 * @param a the [[Expression expression]]
 */
case class NEG(a: Expression) extends RuntimeExpression with UnaryOperation {
  override val operator: String = "-"

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = (scope, IOCost.empty, evaluate())

  override def evaluate()(implicit scope: Scope): Any = {
    LollypopVM.execute(scope, a)._3 match {
      case b: Byte => -b
      case d: Double => -d
      case f: Float => -f
      case i: Int => -i
      case j: java.lang.Byte => if (j != null) -j else null
      case j: java.lang.Double => if (j != null) -j else null
      case j: java.lang.Float => if (j != null) -j else null
      case j: java.lang.Integer => if (j != null) -j else null
      case j: java.lang.Long => if (j != null) -j else null
      case j: java.lang.Short => if (j != null) -j else null
      case l: Long => -l
      case s: Short => -s
      case n: Number => -n.doubleValue()
      case _ => a.dieExpectedNumeric()
    }
  }
}

object NEG extends ExpressionParser {

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts nextIf "-") compiler.nextExpression(ts).map(NEG.apply) ?? ts.dieExpectedNumeric() else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "-"

}